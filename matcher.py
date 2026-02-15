import sqlite3
import imagehash
import os
from difflib import SequenceMatcher

# --- CONFIGURATION ---
SIMILARITY_THRESHOLD = 70.0 

class Matcher:
    def __init__(self, db_path):
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database not found at {db_path}")
        
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row 

    def close(self):
        try:
            self.conn.close()
        except:
            pass

    def fetch_all_files(self):
        cursor = self.conn.execute("SELECT * FROM files")
        # FIX: Filter out files that no longer exist on disk
        valid_files = []
        for row in cursor.fetchall():
            d = dict(row)
            if os.path.exists(d['path']):
                valid_files.append(d)
        return valid_files

    def find_exact_duplicates(self):
        # We fetch all valid files first to ensure existence
        all_files = self.fetch_all_files()
        
        # Group by Exact Hash manually to ensure we only group existing files
        hash_map = {}
        for f in all_files:
            if f['exact_hash']:
                if f['exact_hash'] not in hash_map:
                    hash_map[f['exact_hash']] = []
                hash_map[f['exact_hash']].append(f)
        
        exact_groups = []
        for k, group in hash_map.items():
            if len(group) > 1:
                exact_groups.append(group)
                
        return exact_groups

    def calculate_score(self, file_a, file_b):
        score = 0
        total_weight = 0
        
        # 1. VISUAL
        if file_a['visual_hash'] and file_b['visual_hash']:
            try:
                h1 = imagehash.hex_to_hash(file_a['visual_hash'])
                h2 = imagehash.hex_to_hash(file_b['visual_hash'])
                dist = h1 - h2
                vis_score = max(0, (10 - dist) / 10) * 100
                score += vis_score * 0.50
                total_weight += 0.50
            except: pass

        # 2. AUDIO
        if file_a['audio_hash'] and file_b['audio_hash']:
            if file_a['audio_hash'] == file_b['audio_hash']:
                score += 100 * 0.50
            total_weight += 0.50

        # 3. FILENAME
        name_sim = SequenceMatcher(None, file_a['filename'], file_b['filename']).ratio()
        score += (name_sim * 100) * 0.20
        total_weight += 0.20

        # 4. SIZE
        size_a, size_b = file_a['size'], file_b['size']
        if size_a > 0 and size_b > 0:
            size_sim = (1 - (abs(size_a - size_b) / max(size_a, size_b))) * 100
            score += size_sim * 0.10
            total_weight += 0.10

        # 5. EXTENSION
        if file_a['extension'] == file_b['extension']:
            score += 100 * 0.05
            total_weight += 0.05

        if total_weight == 0: return 0
        return round(score / total_weight, 1)

    def find_fuzzy_matches(self, stop_signal=None):
        files = self.fetch_all_files()
        potential_matches = []
        total = len(files)
        
        for i in range(total):
            if stop_signal and stop_signal():
                self.close()
                return []

            for j in range(i + 1, total):
                f1, f2 = files[i], files[j]
                
                is_aud_1 = f1['extension'] in ['.mp3','.wav','.flac','.m4a','.wma']
                is_aud_2 = f2['extension'] in ['.mp3','.wav','.flac','.m4a','.wma']
                is_vis_1 = f1['extension'] in ['.jpg','.png','.mp4','.avi','.m4v','.mov','.webm','.mkv']
                is_vis_2 = f2['extension'] in ['.jpg','.png','.mp4','.avi','.m4v','.mov','.webm','.mkv']

                if (is_aud_1 != is_aud_2) and (f1['filename'] != f2['filename']): continue
                if (is_vis_1 != is_vis_2) and (f1['filename'] != f2['filename']): continue

                score = self.calculate_score(f1, f2)
                if score >= SIMILARITY_THRESHOLD:
                    potential_matches.append({
                        'file_a': f1['path'],
                        'file_b': f2['path'],
                        'score': score
                    })
        
        self.close()
        return potential_matches