import os
import sqlite3
import hashlib
import concurrent.futures
import threading
from datetime import datetime
import cv2
import imagehash
from PIL import Image
import numpy as np

MAX_SCAN_WORKERS = min(16, max(4, (os.cpu_count() or 4) * 2))

# Suppress OpenCV console spam
os.environ["OPENCV_LOG_LEVEL"] = "OFF"

# Allow massive images to load without crashing
Image.MAX_IMAGE_PIXELS = None

try:
    import librosa
    AUDIO_AVAILABLE = True
except ImportError:
    AUDIO_AVAILABLE = False

# GLOBAL CONFIG: These must match matcher.py logic
IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp', '.tiff', '.tif', '.psd', '.raw'}
VIDEO_EXTS = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.m4v', '.webm', '.ts', '.mts', '.3gp'}
AUDIO_EXTS = {'.mp3', '.wav', '.flac', '.m4a', '.aac', '.ogg', '.wma'}
TEXT_EXTS  = {'.txt', '.md', '.py', '.js', '.json', '.html', '.css', '.c', '.cpp'}

ALL_SUPPORTED_EXTS = IMAGE_EXTS | VIDEO_EXTS | AUDIO_EXTS | TEXT_EXTS


class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.create_table()

    def create_table(self):
        query_files = """
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE,
            filename TEXT,
            extension TEXT,
            size INTEGER,
            mtime REAL,
            ctime REAL,
            exact_hash TEXT,
            visual_hash TEXT,
            audio_hash TEXT,
            scan_date TEXT
        )
        """
        query_roots = """
        CREATE TABLE IF NOT EXISTS scan_roots (
            path TEXT PRIMARY KEY,
            priority INTEGER
        )
        """
        with self.lock:
            self.conn.execute(query_files)
            self.conn.execute(query_roots)
            self.conn.commit()

    def save_roots(self, folder_list):
        with self.lock:
            self.conn.execute("DELETE FROM scan_roots")
            for item in folder_list:
                path = item['path'] if isinstance(item, dict) else item
                prio = item['priority'] if isinstance(item, dict) else 10
                self.conn.execute(
                    "INSERT OR REPLACE INTO scan_roots (path, priority) VALUES (?, ?)",
                    (path, prio)
                )
            self.conn.commit()

    def get_roots(self):
        with self.lock:
            try:
                cursor = self.conn.execute("SELECT path, priority FROM scan_roots ORDER BY path")
                return [{'path': row[0], 'priority': row[1]} for row in cursor.fetchall()]
            except sqlite3.OperationalError:
                try:
                    self.conn.execute("""
                        CREATE TABLE IF NOT EXISTS scan_roots (
                            path TEXT PRIMARY KEY,
                            priority INTEGER
                        )
                    """)
                    self.conn.commit()
                except Exception:
                    pass
                return []

    def get_file_record(self, path):
        with self.lock:
            cursor = self.conn.execute(
                "SELECT mtime, exact_hash, visual_hash, audio_hash FROM files WHERE path = ?",
                (path,)
            )
            return cursor.fetchone()

    def upsert_file(self, data):
        query = """
        INSERT INTO files (path, filename, extension, size, mtime, ctime,
                           exact_hash, visual_hash, audio_hash, scan_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            size=excluded.size, mtime=excluded.mtime, ctime=excluded.ctime,
            exact_hash=excluded.exact_hash, visual_hash=excluded.visual_hash,
            audio_hash=excluded.audio_hash, scan_date=excluded.scan_date
        """
        try:
            with self.lock:
                self.conn.execute(query, data)
                self.conn.commit()
        except Exception as e:
            print(f"DB Write Error: {e}")

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


class Scanner:
    def __init__(self):
        self.db = None

    def generate_exact_hash(self, filepath):
        """MD5 of the full file content."""
        try:
            hasher = hashlib.md5()
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception:
            return None

    def generate_visual_hash(self, filepath, ext):
        """Perceptual hash for images; mid-frame phash for videos."""
        try:
            img = None
            if ext in IMAGE_EXTS:
                img = Image.open(filepath)
                img.load()  # force full decode before releasing file handle
            elif ext in VIDEO_EXTS:
                cap = cv2.VideoCapture(filepath)
                try:
                    if cap.isOpened():
                        total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
                        if total > 0:
                            cap.set(cv2.CAP_PROP_POS_FRAMES, total // 2)
                            ret, frame = cap.read()
                            if ret:
                                img = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
                finally:
                    cap.release()

            if img:
                return str(imagehash.phash(img))
        except Exception:
            pass
        return None

    def generate_audio_hash(self, filepath):
        """
        Chroma-based audio fingerprint via librosa.

        CHANGED: audio files that fail fingerprinting are still indexed using
        their exact hash alone. Previously, a None audio_hash caused the file to
        be skipped entirely, so audio duplicates were never detected at all.
        Returning None here now only means the audio_hash column stays NULL; the
        file is still written and can match on exact_hash or filename.
        """
        if not AUDIO_AVAILABLE:
            return None
        try:
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                y, sr = librosa.load(filepath, duration=30, sr=22050)
                chroma = librosa.feature.chroma_stft(y=y, sr=sr)
                chroma_mean = np.mean(chroma, axis=1)
                fingerprint = ",".join([str(round(x, 1)) for x in chroma_mean])
                return hashlib.md5(fingerprint.encode()).hexdigest()
        except Exception:
            return None

    def process_file(self, filepath):
        """
        Hash and store one file. Returns (success: bool, skipped_reason: str|None).

        CHANGED: unsupported extensions return (False, None) meaning "silently
        ignored" rather than adding them to the user-visible skipped list. Only
        files that are supported but fail processing are counted as skipped.
        Audio files with a failed fingerprint are still indexed via exact_hash.
        """
        try:
            filepath = os.path.abspath(os.path.normpath(filepath))
            if not os.path.exists(filepath):
                return False, None

            ext = os.path.splitext(filepath)[1].lower()

            is_img = ext in IMAGE_EXTS
            is_vid = ext in VIDEO_EXTS
            is_aud = ext in AUDIO_EXTS
            is_txt = ext in TEXT_EXTS

            # Silently ignore unsupported types
            if not any([is_img, is_vid, is_aud, is_txt]):
                return False, None

            stats = os.stat(filepath)
            size  = stats.st_size
            mtime = stats.st_mtime
            ctime = getattr(stats, 'st_ctime', 0)
            filename = os.path.basename(filepath)

            # Skip re-hashing if the file hasn't changed
            existing = self.db.get_file_record(filepath)
            if existing and round(existing[0], 2) == round(mtime, 2):
                return True, None

            exact_hash  = self.generate_exact_hash(filepath)
            visual_hash = None
            audio_hash  = None

            if is_img or is_vid:
                visual_hash = self.generate_visual_hash(filepath, ext)
                if visual_hash is None:
                    # Can't produce a perceptual hash -- skip and report
                    return False, filepath

            if is_aud:
                # Audio hash is best-effort; None just means no fingerprint column
                audio_hash = self.generate_audio_hash(filepath)

            data = (
                filepath, filename, ext, size, mtime, ctime,
                exact_hash, visual_hash, audio_hash,
                datetime.now().isoformat()
            )
            self.db.upsert_file(data)
            return True, None

        except Exception:
            return False, filepath

    def scan_directory(self, folder_list, db_path, stop_signal=None, progress_callback=None):
        """Scan directories and dispatch worker threads for hashing."""
        print("Starting Scan")
        self.db = DatabaseManager(db_path)
        self.db.save_roots(folder_list)

        db_filename = os.path.basename(db_path)
        ignored_files = {
            db_filename,
            db_filename + "-shm",
            db_filename + "-wal",
            "duplicate_index.db",
        }

        files_to_process = []
        for root_dir in folder_list:
            if stop_signal and stop_signal():
                break
            path_str = root_dir['path'] if isinstance(root_dir, dict) else root_dir
            path_str = os.path.abspath(os.path.normpath(path_str))

            for root, dirs, files in os.walk(path_str):
                if stop_signal and stop_signal():
                    break
                for file in files:
                    if file in ignored_files:
                        continue
                    full = os.path.join(root, file)
                    ext = os.path.splitext(file)[1].lower()
                    # Pre-filter unsupported types before even queuing them
                    if ext in ALL_SUPPORTED_EXTS:
                        files_to_process.append(full)

        total_files = len(files_to_process)
        print(f"Found {total_files} supported files. Processing...")

        processed_count = 0
        skipped_files_list = []

        worker_count = min(MAX_SCAN_WORKERS, max(1, (os.cpu_count() or 4) * 2))
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=worker_count)
        future_to_file = {}

        try:
            for fp in files_to_process:
                future_to_file[executor.submit(self.process_file, fp)] = fp

            for future in concurrent.futures.as_completed(future_to_file):
                if stop_signal and stop_signal():
                    break

                try:
                    success, skip_path = future.result()
                    if skip_path:
                        skipped_files_list.append(skip_path)
                except Exception:
                    skipped_files_list.append(future_to_file[future])

                processed_count += 1
                if progress_callback and processed_count % 10 == 0:
                    progress_callback(processed_count, total_files, len(skipped_files_list))
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        self.db.close()
        if stop_signal and stop_signal():
            return None, []

        print("Scan Complete")
        return db_path, skipped_files_list
