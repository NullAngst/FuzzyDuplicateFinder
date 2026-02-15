import os
import sqlite3
import hashlib
import time
import concurrent.futures
import threading
from datetime import datetime
import cv2
import imagehash
from PIL import Image
import numpy as np

# Suppress OpenCV console spam
os.environ["OPENCV_LOG_LEVEL"] = "OFF"

# FIX 1: Allow massive images (AI upscales) without crashing
Image.MAX_IMAGE_PIXELS = None

try:
    import librosa
    AUDIO_AVAILABLE = True
except ImportError:
    AUDIO_AVAILABLE = False

IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp', '.tiff'}
VIDEO_EXTS = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.m4v', '.webm', '.ts', '.mts', '.3gp'}
AUDIO_EXTS = {'.mp3', '.wav', '.flac', '.m4a', '.aac', '.ogg', '.wma'}
TEXT_EXTS = {'.txt', '.md', '.py', '.js', '.json', '.html', '.css', '.c', '.cpp'}

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.create_table()

    def create_table(self):
        query = """
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
        with self.lock: 
            self.conn.execute(query)
            self.conn.commit()

    def get_file_record(self, path):
        with self.lock:
            cursor = self.conn.execute("SELECT mtime, exact_hash, visual_hash, audio_hash FROM files WHERE path = ?", (path,))
            return cursor.fetchone()

    def upsert_file(self, data):
        query = """
        INSERT INTO files (path, filename, extension, size, mtime, ctime, exact_hash, visual_hash, audio_hash, scan_date)
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
        except:
            pass

class Scanner:
    def __init__(self):
        self.db = None 

    def generate_exact_hash(self, filepath):
        try:
            hasher = hashlib.md5()
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(65536), b""): # Increased chunk size for speed
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception:
            return None

    def generate_visual_hash(self, filepath, ext):
        try:
            img = None
            if ext in IMAGE_EXTS:
                img = Image.open(filepath)
            elif ext in VIDEO_EXTS:
                # Wrap video capture in generic try/except to prevent console spam
                try:
                    cap = cv2.VideoCapture(filepath)
                    if cap.isOpened():
                        total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
                        # Safety check for empty/corrupt video containers
                        if total > 0:
                            cap.set(cv2.CAP_PROP_POS_FRAMES, total // 2)
                            ret, frame = cap.read()
                            if ret:
                                img = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
                    cap.release()
                except Exception:
                    pass
            
            if img: return str(imagehash.phash(img))
        except Exception:
            pass
        return None

    def generate_audio_hash(self, filepath):
        if not AUDIO_AVAILABLE: return None
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
        try:
            filepath = os.path.abspath(os.path.normpath(filepath))
            
            # FIX 2: Check existence immediately to prevent ghosts
            if not os.path.exists(filepath):
                return False

            stats = os.stat(filepath)
            size = stats.st_size
            mtime = stats.st_mtime
            ctime = getattr(stats, 'st_ctime', 0)
            filename = os.path.basename(filepath)
            ext = os.path.splitext(filepath)[1].lower()

            is_img = ext in IMAGE_EXTS
            is_vid = ext in VIDEO_EXTS
            is_aud = ext in AUDIO_EXTS
            is_txt = ext in TEXT_EXTS

            if not any([is_img, is_vid, is_aud, is_txt]): return False

            existing = self.db.get_file_record(filepath)
            if existing and round(existing[0], 2) == round(mtime, 2): 
                return True # Skipped (Already Indexed)

            exact_hash = self.generate_exact_hash(filepath)
            visual_hash = None
            audio_hash = None

            if is_img or is_vid: visual_hash = self.generate_visual_hash(filepath, ext)
            if is_aud: audio_hash = self.generate_audio_hash(filepath)

            data = (filepath, filename, ext, size, mtime, ctime, exact_hash, visual_hash, audio_hash, datetime.now().isoformat())
            self.db.upsert_file(data)
            return True # Processed successfully
            
        except Exception:
            return False # Error

    def scan_directory(self, folder_list, db_path, stop_signal=None, progress_callback=None):
        """
        progress_callback: function(current, total, skipped_count)
        """
        print(f"--- Starting Scan ---")
        self.db = DatabaseManager(db_path)
        files_to_process = []
        
        # Phase 1: Indexing
        for root_dir in folder_list:
            if stop_signal and stop_signal(): break
            # Handle dict objects from new priority system or plain strings
            path_str = root_dir['path'] if isinstance(root_dir, dict) else root_dir
            
            path_str = os.path.abspath(os.path.normpath(path_str))
            for root, dirs, files in os.walk(path_str):
                if stop_signal and stop_signal(): break
                for file in files:
                    if file == "duplicate_index.db": continue
                    files_to_process.append(os.path.join(root, file))

        total_files = len(files_to_process)
        print(f"Found {total_files} files. Processing...")
        
        # Phase 2: Processing
        processed_count = 0
        skipped_count = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            # We map futures to filepaths so we can track progress
            future_to_file = {executor.submit(self.process_file, fp): fp for fp in files_to_process}
            
            for future in concurrent.futures.as_completed(future_to_file):
                if stop_signal and stop_signal(): break
                
                try:
                    success = future.result()
                    if not success:
                        skipped_count += 1
                except Exception:
                    skipped_count += 1
                
                processed_count += 1
                
                # Report Progress back to UI
                if progress_callback and processed_count % 10 == 0:
                    progress_callback(processed_count, total_files, skipped_count)

        self.db.close()
        
        if stop_signal and stop_signal():
            return None
            
        print("--- Scan Complete ---")
        return db_path