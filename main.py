import os
import time
import json
import logging
import requests
from lxml import etree
from datetime import datetime
from pathlib import Path
from typing import overload, Any, Literal

# --- 定数定義 ---
# 実行時間制限 (分)
EXECUTION_LIMIT_MINUTES =55
# APIエンドポイント
LATEST_VIDEO_API_URL = "https://snapshot.search.nicovideo.jp/api/v2/snapshot/video/contents/search"
GETTHUMBINFO_API_URL = "https://ext.nicovideo.jp/api/getthumbinfo/"
# データ保存用ルートディレクトリ
DATA_ROOT = Path("./")
# 進捗管理ファイル
PROGRESS_FILE = Path("progress.json")
# 対象とする動画プレフィックス
VIDEO_PREFIX = "sm"
# 最も古い動画ID (これより小さいIDはスキップ)
OLDEST_VIDEO_ID = 9
# リクエスト間の待機時間 (秒)
REQUEST_WAIT_SECONDS = 0
# APIリクエストのリトライ設定
RETRY_COUNT = 5
RETRY_WAIT_SECONDS = 10

class NicoArchiver:
    """
    ニコニコ動画の情報をアーカイブするクラス。
    """

    def __init__(self):
        self.start_time = time.time()
        self.session = requests.Session()
        self.progress_data = self._load_progress()
        self._setup_logging()

    def _setup_logging(self):
        """ロギングを設定する。"""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    def _get_path_for_id(self, prefix: str, item_id: int, extension: str) -> Path:
        """
        IDに基づいて階層的な保存パスを生成する。

        例: (prefix="sm", item_id=50567, extension="xml")
            -> ./sm/1-1000000/50001-60000/50501-50600/50567.xml
        """
        item_id_m1 = item_id - 1
        l1 = (item_id_m1 // 1000000) * 1000000 + 1
        l2 = (item_id_m1 // 10000) * 10000 + 1
        l3 = (item_id_m1 // 100) * 100 + 1
        
        dir_path = DATA_ROOT / prefix / f"{l1}-{l1+999999}" / f"{l2}-{l2+9999}" / f"{l3}-{l3+99}"
        return dir_path / f"{item_id}.{extension}"

    def _load_progress(self) -> dict:
        """進捗管理ファイルを読み込む。"""
        if not PROGRESS_FILE.exists():
            logging.info(f"進捗ファイル '{PROGRESS_FILE}' が見つかりません。新しいファイルを作成します。")
            return {
                "sm": {"processed_ranges": [], "last_known_id": None},
                "nm": {"processed_ranges": [], "last_known_id": None},
                "so": {"processed_ranges": [], "last_known_id": None},
            }
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_progress(self):
        """進捗管理ファイルに保存する。"""
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(self.progress_data, f, indent=2, ensure_ascii=False)
        logging.info(f"進捗を '{PROGRESS_FILE}' に保存しました。")

    # --- ▼▼▼ ここからが修正箇所 ▼▼▼ ---

    @overload
    def _fetch_with_retry(self, url: str, *, is_binary: Literal[True], params: dict[str, Any] | None = None) -> bytes | None: ...
    @overload
    def _fetch_with_retry(self, url: str, *, is_binary: Literal[False] = False, params: dict[str, Any] | None = None) -> str | None: ...

    def _fetch_with_retry(self, url: str, *, is_binary: bool = False, params: dict[str, Any] | None = None) -> bytes | str | None:
        """リトライロジック付きでURLからコンテンツを取得する。"""
        for i in range(RETRY_COUNT):
            try:
                response = self.session.get(url, params=params, timeout=15)
                if response.status_code == 200:
                    return response.content if is_binary else response.text
                elif response.status_code == 404:
                    logging.warning(f"取得失敗 (404 Not Found): {url}")
                    return None
                else:
                    logging.warning(f"取得失敗 (Status: {response.status_code}), {i+1}/{RETRY_COUNT} 回目のリトライ待機...: {url}")
                    time.sleep(RETRY_WAIT_SECONDS)
            except requests.RequestException as e:
                logging.error(f"リクエストエラー: {e}, {i+1}/{RETRY_COUNT} 回目のリトライ待機...: {url}")
                time.sleep(RETRY_WAIT_SECONDS)
        
        logging.error(f"リトライ上限に達しました。取得をスキップします: {url}")
        return None

    def get_latest_video_id(self) -> int | None:
        """スナップショット検索APIから最新のsm動画IDを取得する。"""
        params = {
            "q": "",
            "targets": "title",
            "fields": "contentId",
            "_sort": "-startTime",
            "_limit": 1,
            "_context": "nico-archiver"
        }
        logging.info("最新の動画IDを取得しています...")
        # paramsをキーワード引数として渡すように修正
        response_text = self._fetch_with_retry(LATEST_VIDEO_API_URL, params=params)
        if not response_text:
            return None
        
        try:
            data = json.loads(response_text)
            content_id = data["data"][0]["contentId"]
            latest_id = int(content_id.replace(VIDEO_PREFIX, ""))
            logging.info(f"最新の動画ID: {VIDEO_PREFIX}{latest_id}")
            return latest_id
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            logging.error(f"最新動画IDの解析に失敗しました: {e}")
            return None

    # --- ▲▲▲ ここまでが修正箇所 ▲▲▲ ---

    def _merge_ranges(self, ranges: list[list[int]]) -> list[list[int]]:
        """重なり合うIDの範囲を結合する。"""
        if not ranges:
            return []
        
        sorted_ranges = sorted([sorted(r) for r in ranges], key=lambda x: x[0])
        
        merged = [sorted_ranges[0]]
        for current_start, current_end in sorted_ranges[1:]:
            last_start, last_end = merged[-1]
            if current_start <= last_end + 1:
                merged[-1] = [last_start, max(last_end, current_end)]
            else:
                merged.append([current_start, current_end])
        return merged

    def run(self):
        """アーカイブ処理のメインループを実行する。"""
        latest_id = self.get_latest_video_id()
        if latest_id is None:
            logging.error("最新IDが取得できなかったため、処理を終了します。")
            return

        latest_id = latest_id - 10000
        self.progress_data[VIDEO_PREFIX]["last_known_id"] = latest_id
        
        processed_ranges = self._merge_ranges(self.progress_data[VIDEO_PREFIX]["processed_ranges"])
        
        if processed_ranges and len(processed_ranges) == 1 and processed_ranges[0][0] <= OLDEST_VIDEO_ID and processed_ranges[0][1] >= latest_id:
            logging.info(f"ID {OLDEST_VIDEO_ID} から {latest_id} までの全範囲が取得済みです。進捗をリセットします。")
            processed_ranges = []
        
        self.progress_data[VIDEO_PREFIX]["processed_ranges"] = processed_ranges
        
        current_id = latest_id
        start_id_this_run = latest_id
        end_id_this_run = -1

        logging.info("動画情報の取得を開始します...")
        
        while current_id >= OLDEST_VIDEO_ID:
            if time.time() - self.start_time > EXECUTION_LIMIT_MINUTES * 60:
                logging.info("実行時間が上限に達しました。処理を終了します。")
                break

            skipped = False
            for start, end in sorted(processed_ranges, key=lambda x: x[0], reverse=True):
                if start <= current_id <= end:
                    logging.info(f"ID {current_id} は処理済み範囲 [{start}, {end}] に含まれるため、{start-1}までスキップします。")
                    current_id = start - 1
                    skipped = True
                    break
            if skipped:
                continue
            
            if current_id < OLDEST_VIDEO_ID:
                break
            
            end_id_this_run = current_id
            video_id_full = f"{VIDEO_PREFIX}{current_id}"
            if current_id % 1000 == 0:
                logging.info(f"処理中(n%1000): {video_id_full}")

            thumbinfo_url = f"{GETTHUMBINFO_API_URL}{video_id_full}"
            # is_binary=False(デフォルト)なので、返り値は str | None
            xml_text = self._fetch_with_retry(thumbinfo_url)

            if xml_text:
                xml_path = self._get_path_for_id(VIDEO_PREFIX, current_id, "xml")
                xml_path.parent.mkdir(parents=True, exist_ok=True)
                # xml_textはstr型なのでエラーにならない
                with open(xml_path, "w", encoding="utf-8") as f:
                    f.write(xml_text)
                
                try:
                    # xml_textがstrなので.encode()は常に成功する
                    root = etree.fromstring(xml_text.encode('utf-8'))
                    if root.get("status") == "ok":
                        thumb_url = root.findtext(".//thumbnail_url")
                        user_icon_url = root.findtext(".//user_icon_url")
                        user_id = root.findtext(".//user_id")

                        if thumb_url:
                            thumb_img = None
                            # URLのファイル名部分を取得
                            basename = os.path.basename(thumb_url)
                            
                            # ファイル名に '.' が含まれる形式 (例: .../id/id.timestamp) のみ.Lを試す
                            if '.' in basename:
                                large_thumb_url = thumb_url + '.L'
                                thumb_img = self._fetch_with_retry(large_thumb_url, is_binary=True)
                                
                                # 高解像度版の取得に失敗した場合、通常版にフォールバック
                                if not thumb_img:
                                    logging.info(f"高解像度版の取得に失敗。: {thumb_url}")
                                    thumb_img = self._fetch_with_retry(thumb_url, is_binary=True)
                            else:
                                # .Lを付けられない形式のURLの場合 (例: .../id/id)
                                thumb_img = self._fetch_with_retry(thumb_url, is_binary=True)
                            
                            # 最終的に画像が取得できていれば保存
                            if thumb_img:
                                thumb_path = self._get_path_for_id(VIDEO_PREFIX, current_id, "jpg")
                                with open(thumb_path, "wb") as f:
                                    f.write(thumb_img)
                            else:
                                logging.warning(f"{video_id_full} のサムネイル画像の取得に失敗しました。")
                        
                        """ iconの保存は容量の都合上つらい
                        if user_icon_url and user_id and "defaults" not in user_icon_url:
                            # is_binary=Trueなので、返り値は bytes | None
                            icon_img = self._fetch_with_retry(user_icon_url, is_binary=True)
                            if icon_img:
                                icon_path = self._get_path_for_id("usericon", int(user_id), "jpg")
                                icon_path.parent.mkdir(parents=True, exist_ok=True)
                                # icon_imgはbytes型なのでエラーにならない
                                with open(icon_path, "wb") as f:
                                    f.write(icon_img)
                        """
                        
                except etree.XMLSyntaxError:
                    logging.warning(f"{video_id_full} のXMLパースに失敗しました。")

            current_id -= 1
            time.sleep(REQUEST_WAIT_SECONDS)
        else:
            logging.info("最も古い動画IDまで到達しました。")

        if end_id_this_run != -1:
            new_range = [end_id_this_run, start_id_this_run]
            logging.info(f"今回の実行で処理したID範囲: {new_range}")
            self.progress_data[VIDEO_PREFIX]["processed_ranges"].append(new_range)
            self.progress_data[VIDEO_PREFIX]["processed_ranges"] = self._merge_ranges(self.progress_data[VIDEO_PREFIX]["processed_ranges"])
        
        self._save_progress()
        logging.info("すべての処理が完了しました。")

if __name__ == "__main__":
    archiver = NicoArchiver()
    archiver.run()