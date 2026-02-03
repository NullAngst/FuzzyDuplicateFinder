import os
import sqlite3
import hashlib
import time
import concurrent.futures
from datetime import datetime
import cv2
import imagehash
from PIL import Image
import numpy as np

# Try importing librosa for audio analysis
try:
    import librosa
    AUDIO_AVAILABLE = True
except ImportError:
    AUDIO_AVAILABLE = False
    print("Warning: 'librosa' not found. Audio sonic analysis will be skipped.")
    print("Run: pip install librosa numpy")

# --- CONFIGURATION ---
# Supported Extensions
IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp'}
VIDEO_EXTS = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv'}
AUDIO_EXTS = {'.mp3', '.wav', '.flac', '.m4a', '.aac', '.ogg'}
TEXT_EXTS = {'.txt', '.md', '.py', '.js', '.json', '.html'}

class DatabaseManager:
    """
    Handles all SQLite interactions.
    Now supports connecting to any arbitrary DB path provided by the Scanner.
    """
    def __init__(self, db_path):
        self.db_path = db_path
        # timeout=30 and isolation_level=None prevent "Database is locked" errors during threading
        self.conn = sqlite3.connect(
            db_path, 
            timeout=30, 
            check_same_thread=False,
            isolation_level=None
        )
        # WAL mode improves write performance during concurrent access
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
            exact_hash TEXT,
            visual_hash TEXT,
            audio_hash TEXT,
            scan_date TEXT
        )
        """
        self.conn.execute(query)
        self.conn.commit()

    def get_file_record(self, path):
        """Fetch existing metadata for a file path."""
        cursor = self.conn.execute(
            "SELECT mtime, exact_hash, visual_hash, audio_hash FROM files WHERE path = ?", 
            (path,)
        )
        return cursor.fetchone()

    def upsert_file(self, data):
        """Insert a new record or update an existing one if the path exists."""
        query = """
        INSERT INTO files (path, filename, extension, size, mtime, exact_hash, visual_hash, audio_hash, scan_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            size=excluded.size,
            mtime=excluded.mtime,
            exact_hash=excluded.exact_hash,
            visual_hash=excluded.visual_hash,
            audio_hash=excluded.audio_hash,
            scan_date=excluded.scan_date
        """
        try:
            self.conn.execute(query, data)
            self.conn.commit()
        except Exception as e:
            print(f"DB Write Error: {e}")

    def close(self):
        self.conn.close()

class Scanner:
    def __init__(self):
        self.db = None # Database connection is established only after we know the target folder

    def generate_exact_hash(self, filepath):
        """Generates MD5 hash for binary identical checks."""
        try:
            hasher = hashlib.md5()
            with open(filepath, 'rb') as f:
                # Read in 4KB chunks to avoid memory issues with large files
                for chunk in iter(lambda: f.read(4096), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception:
            return None

    def generate_visual_hash(self, filepath, ext):
        """Generates pHash (Perceptual Hash) for images and video frames."""
        try:
            img = None
            if ext in IMAGE_EXTS:
                img = Image.open(filepath)
            elif ext in VIDEO_EXTS:
                # Extract a frame from the middle of the video
                cap = cv2.VideoCapture(filepath)
                if cap.isOpened():
                    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
                    cap.set(cv2.CAP_PROP_POS_FRAMES, total // 2)
                    ret, frame = cap.read()
                    cap.release()
                    if ret:
                        img = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
            
            if img:
                return str(imagehash.phash(img))
        except Exception:
            pass
        return None

    def generate_audio_hash(self, filepath):
        """
        Generates a 'sonic fingerprint' using Chroma features.
        We analyze the first 30 seconds to create a pitch-based hash.
        """
        if not AUDIO_AVAILABLE: return None
        try:
            # Load only first 30s to keep it fast
            y, sr = librosa.load(filepath, duration=30, sr=22050)
            chroma = librosa.feature.chroma_stft(y=y, sr=sr)
            # Average pitch class usage
            chroma_mean = np.mean(chroma, axis=1)
            # Rounding to 1 decimal allows for slight audio compression differences
            fingerprint = ",".join([str(round(x, 1)) for x in chroma_mean])
            return hashlib.md5(fingerprint.encode()).hexdigest()
        except Exception as e:
            print(f"Audio Error {os.path.basename(filepath)}: {e}")
            return None

    def process_file(self, filepath):
        """Worker function that processes a single file."""
        try:
            # CRITICAL: Normalize path to prevent duplicates (C:/Path vs C:\Path)
            filepath = os.path.abspath(os.path.normpath(filepath))

            stats = os.stat(filepath)
            size = stats.st_size
            mtime = stats.st_mtime
            filename = os.path.basename(filepath)
            ext = os.path.splitext(filepath)[1].lower()

            # Skip unsupported files
            is_img = ext in IMAGE_EXTS
            is_vid = ext in VIDEO_EXTS
            is_aud = ext in AUDIO_EXTS
            is_txt = ext in TEXT_EXTS

            if not any([is_img, is_vid, is_aud, is_txt]):
                return

            # Check if we need to re-scan this file
            existing = self.db.get_file_record(filepath)
            
            # Using round(mtime, 2) handles float precision differences between Python runs
            if existing and round(existing[0], 2) == round(mtime, 2):
                return 

            print(f"Processing: {filename}")
            
            exact_hash = self.generate_exact_hash(filepath)
            visual_hash = None
            audio_hash = None

            if is_img or is_vid:
                visual_hash = self.generate_visual_hash(filepath, ext)
            
            if is_aud:
                audio_hash = self.generate_audio_hash(filepath)

            data = (filepath, filename, ext, size, mtime, exact_hash, visual_hash, audio_hash, datetime.now().isoformat())
            self.db.upsert_file(data)
            
        except PermissionError:
            print(f"Permission Denied: {filepath}")
        except Exception as e:
            print(f"Error on {filepath}: {e}")

    def scan_directory(self, root_dir, db_output_path=None):
        """
        Main entry point. 
        db_output_path: Optional custom location for the DB. 
        Default behavior: Stores 'duplicate_index.db' inside the scanned directory.
        """
        # Normalize the root directory path
        root_dir = os.path.abspath(os.path.normpath(root_dir))
        
        # Determine where to save the DB
        if db_output_path:
            db_path = db_output_path
        else:
            db_path = os.path.join(root_dir, "duplicate_index.db")

        print(f"--- Starting Scan of {root_dir} ---")
        print(f"Index Location: {db_path}")

        # Initialize DB connection
        self.db = DatabaseManager(db_path)

        files_to_process = []
        for root, dirs, files in os.walk(root_dir):
            for file in files:
                # CRITICAL: Do not index the database file itself!
                if file == "duplicate_index.db":
                    continue
                
                full_path = os.path.join(root, file)
                files_to_process.append(full_path)

        print(f"Found {len(files_to_process)} files. Processing...")
        
        # Multithreaded processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(self.process_file, files_to_process)
            
        print("--- Scan Complete ---")
        return db_path

if __name__ == "__main__":
    scanner = Scanner()
    target_dir = input("Enter directory path to scan: ").strip('"')
    if os.path.exists(target_dir):
        start_time = time.time()
        scanner.scan_directory(target_dir)
        print(f"Duration: {round(time.time() - start_time, 2)} seconds")
    else:
        print("Invalid directory path.")