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
        try: self.conn.close()
        except: pass

    def fetch_all_files(self):
        cursor = self.conn.execute("SELECT * FROM files")
        return [dict(row) for row in cursor.fetchall()]

    def find_exact_duplicates(self):
        query = """
        SELECT exact_hash, COUNT(*) as count 
        FROM files 
        WHERE exact_hash IS NOT NULL 
        GROUP BY exact_hash 
        HAVING count > 1
        """
        cursor = self.conn.execute(query)
        exact_groups = []
        for row in cursor.fetchall():
            file_hash = row['exact_hash']
            files_cursor = self.conn.execute("SELECT path, filename FROM files WHERE exact_hash = ?", (file_hash,))
            exact_groups.append([dict(f) for f in files_cursor.fetchall()])
        return exact_groups

    def calculate_score(self, f1, f2):
        score = 0
        total_weight = 0

        # 1. Visual Hash (60%)
        if f1['visual_hash'] and f2['visual_hash']:
            try:
                h1 = imagehash.hex_to_hash(f1['visual_hash'])
                h2 = imagehash.hex_to_hash(f2['visual_hash'])
                dist = h1 - h2
                sim = max(0, (64 - dist) / 64) * 100
                score += sim * 0.60
                total_weight += 0.60
            except: pass

        # 2. Filename Similarity (20%)
        if f1['filename'] and f2['filename']:
            name_sim = SequenceMatcher(None, f1['filename'], f2['filename']).ratio() * 100
            score += name_sim * 0.20
            total_weight += 0.20
        
        # 3. Size Similarity (10%)
        size_a, size_b = f1['size'], f2['size']
        if size_a > 0 and size_b > 0:
            size_sim = (1 - (abs(size_a - size_b) / max(size_a, size_b))) * 100
            score += size_sim * 0.10
            total_weight += 0.10

        # 4. Audio Hash (Optional - 60% if visual is missing)
        if f1['audio_hash'] and f2['audio_hash'] and total_weight < 0.5:
             # Simple exact check for now, can be improved to hamming later
             if f1['audio_hash'] == f2['audio_hash']:
                 score += 100 * 0.60
             else:
                 score += 0
             total_weight += 0.60

        if total_weight == 0: return 0
        return round(score / total_weight, 1)

    # UPDATED: Added progress_callback
    def find_fuzzy_matches(self, stop_signal=None, progress_callback=None):
        files = self.fetch_all_files()
        potential_matches = []
        n = len(files)
        
        # Calculate total comparisons for progress bar: (n * (n-1)) / 2
        total_comparisons = (n * (n - 1)) // 2
        current_comparison = 0
        
        # Optimization: Pre-sort or filter to reduce checks if needed, 
        # but for now we run full N^2 with progress updates.
        
        for i in range(n):
            if stop_signal and stop_signal(): break
            
            # Optimization: Batch update progress to avoid UI flooding
            if progress_callback and i % 5 == 0:
                # Roughly estimate progress based on outer loop to save math
                # or use precise counter
                progress_callback(current_comparison, total_comparisons)

            for j in range(i + 1, n):
                current_comparison += 1
                
                f1, f2 = files[i], files[j]
                
                # OPTIMIZATION: Quick Type Check
                is_aud_1 = f1['extension'] in {'.mp3','.wav','.flac','.m4a','.wma'}
                is_aud_2 = f2['extension'] in {'.mp3','.wav','.flac','.m4a','.wma'}
                is_vis_1 = f1['extension'] in {'.jpg','.png','.mp4','.avi','.m4v','.mov'}
                is_vis_2 = f2['extension'] in {'.jpg','.png','.mp4','.avi','.m4v','.mov'}

                # Skip if types differ drastically (Audio vs Image)
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