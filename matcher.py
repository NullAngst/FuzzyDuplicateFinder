import sqlite3
import imagehash
import os
import concurrent.futures
from difflib import SequenceMatcher

# Configuration variables
SIMILARITY_THRESHOLD = 70.0
MAX_MATCH_WORKERS = max(1, min(8, os.cpu_count() or 4))

# Must match Scanner Engine extensions
VISUAL_EXTS = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp', '.tiff', '.tif', '.psd', '.raw',
               '.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.m4v', '.webm', '.ts', '.mts', '.3gp'}
AUDIO_EXTS = {'.mp3', '.wav', '.flac', '.m4a', '.aac', '.ogg', '.wma'}
TEXT_EXTS  = {'.txt', '.md', '.py', '.js', '.json', '.html', '.css', '.c', '.cpp'}


def _pair_range_count(start, end, n):
    """Count total pair comparisons for rows [start, end) against the rest."""
    total = 0
    for i in range(start, end):
        total += max(0, n - i - 1)
    return total


def _file_type_group(ext):
    """Return a coarse type bucket so we never compare across media categories."""
    if ext in VISUAL_EXTS: return 'visual'
    if ext in AUDIO_EXTS:  return 'audio'
    if ext in TEXT_EXTS:   return 'text'
    return 'other'


def _compare_range(files, start, end, similarity_threshold, exact_hashes):
    """
    Compare files[start:end] against files[i+1:n].

    exact_hashes: set of exact_hash values that already have exact duplicates.
    Pairs whose files share the same exact_hash are skipped here because they
    will already be reported as EXACT matches, not fuzzy ones.

    NOTE: stop_signal is intentionally absent. This function runs inside a
    subprocess (ProcessPoolExecutor) and bound methods cannot be pickled.
    Early exit on stop is handled by the caller between chunk completions.
    """
    matches = []
    n = len(files)

    for i in range(start, end):
        f1 = files[i]
        type1 = _file_type_group(f1['extension'])

        for j in range(i + 1, n):
            f2 = files[j]

            # Never cross media type boundaries
            if type1 != _file_type_group(f2['extension']):
                continue

            # Skip pairs that are already exact duplicates
            h1 = f1.get('exact_hash')
            h2 = f2.get('exact_hash')
            if h1 and h2 and h1 == h2:
                continue

            score = _calculate_score_local(f1, f2)
            if score >= similarity_threshold:
                matches.append({
                    'file_a': f1['path'],
                    'file_b': f2['path'],
                    'score': score,
                })

    return matches


def _calculate_score_local(f1, f2):
    """Calculate a weighted similarity score between two file records."""
    is_visual = f1['extension'] in VISUAL_EXTS
    is_audio  = f1['extension'] in AUDIO_EXTS
    has_visual_hashes = f1.get('visual_hash') and f2.get('visual_hash')
    has_audio_hashes  = f1.get('audio_hash')  and f2.get('audio_hash')

    # Hard-fail if the expected content hash is missing
    if is_visual and not has_visual_hashes:
        return 0
    if is_audio and not has_audio_hashes:
        return 0

    score = 0.0
    total_weight = 0.0

    # 1. Visual hash (perceptual) -- 50% weight
    if has_visual_hashes:
        try:
            h1 = imagehash.hex_to_hash(f1['visual_hash'])
            h2 = imagehash.hex_to_hash(f2['visual_hash'])
            dist = h1 - h2
            sim = max(0.0, (10 - dist) / 10) * 100
            score += sim * 0.50
            total_weight += 0.50
        except Exception:
            pass

    # 2. Audio fingerprint -- 50% weight (binary match only)
    if has_audio_hashes:
        if f1['audio_hash'] == f2['audio_hash']:
            score += 100 * 0.50
        total_weight += 0.50

    # 3. Filename similarity -- 20% weight
    if f1.get('filename') and f2.get('filename'):
        name_sim = SequenceMatcher(None, f1['filename'], f2['filename']).ratio() * 100
        score += name_sim * 0.20
        total_weight += 0.20

    # 4. File-size similarity -- 10% weight
    size_a, size_b = f1.get('size', 0), f2.get('size', 0)
    if size_a > 0 and size_b > 0:
        size_sim = (1 - abs(size_a - size_b) / max(size_a, size_b)) * 100
        score += size_sim * 0.10
        total_weight += 0.10

    # 5. Extension match -- 5% weight
    if f1.get('extension') == f2.get('extension'):
        score += 100 * 0.05
        total_weight += 0.05

    if total_weight == 0:
        return 0
    return round(score / total_weight, 1)


class Matcher:
    def __init__(self, db_path):
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database not found at {db_path}")
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def fetch_all_files(self):
        """Return all indexed file records that still exist on disk."""
        cursor = self.conn.execute("SELECT * FROM files")
        valid = []
        for row in cursor.fetchall():
            d = dict(row)
            if os.path.exists(d['path']):
                valid.append(d)
        return valid

    def find_exact_duplicates(self):
        """Group files that share an identical MD5 hash."""
        all_files = self.fetch_all_files()
        hash_map = {}
        for f in all_files:
            h = f.get('exact_hash')
            if h:
                hash_map.setdefault(h, []).append(f)
        return [group for group in hash_map.values() if len(group) > 1]

    def find_fuzzy_matches(self, stop_signal=None, progress_callback=None, max_workers=None):
        """
        Multi-process fuzzy comparison across all indexed files.

        FIXED BUG (original): The original code passed a list of (future, start, end)
        tuples directly to as_completed(), raising AttributeError on ._condition and
        silently killing all fuzzy matching. Fixed by maintaining a plain future list
        for as_completed and a separate dict for progress tracking.

        FIXED BUG (threading): ThreadPoolExecutor gave no real parallelism here
        because _calculate_score_local is CPU-bound pure Python and the GIL
        serializes all threads. Switched to ProcessPoolExecutor so each worker runs
        in its own interpreter with its own GIL and comparisons execute in parallel.

        stop_signal is checked between chunk completions in the outer loop but is
        NOT passed into _compare_range -- bound methods cannot be pickled and
        therefore cannot cross the process boundary.
        """
        files = self.fetch_all_files()
        n = len(files)

        if n < 2:
            self.close()
            return []

        # Collect hashes that appear more than once so fuzzy can skip exact pairs
        hash_counts = {}
        for f in files:
            h = f.get('exact_hash')
            if h:
                hash_counts[h] = hash_counts.get(h, 0) + 1
        exact_hashes = {h for h, count in hash_counts.items() if count > 1}

        total_comparisons = (n * (n - 1)) // 2
        current_comparison = 0

        worker_count = (
            max_workers
            if max_workers and max_workers > 0
            else min(MAX_MATCH_WORKERS, max(1, n - 1))
        )

        # Each chunk submission serializes the full files list and sends it to a
        # worker process. Fewer, larger chunks keep that overhead manageable while
        # still giving the scheduler enough work units for reasonable load-balancing.
        # 4 chunks per worker is the practical sweet spot.
        chunk_count = min(worker_count * 4, n - 1)
        chunk_size  = max(1, -(-( n - 1) // chunk_count))  # ceiling division

        ranges = []
        start = 0
        while start < n - 1:
            end = min(n - 1, start + chunk_size)
            ranges.append((start, end))
            start = end

        potential_matches = []
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=worker_count)

        future_list = []
        future_to_range = {}

        try:
            for s, e in ranges:
                if stop_signal and stop_signal():
                    break
                future = executor.submit(
                    _compare_range, files, s, e,
                    SIMILARITY_THRESHOLD, exact_hashes
                    # stop_signal intentionally omitted -- not picklable
                )
                future_list.append(future)
                future_to_range[future] = (s, e)

            for future in concurrent.futures.as_completed(future_list):
                if stop_signal and stop_signal():
                    for f in future_list:
                        if not f.done():
                            f.cancel()
                    break

                try:
                    matches = future.result(timeout=120)
                    potential_matches.extend(matches)
                except concurrent.futures.CancelledError:
                    pass
                except Exception:
                    pass

                s, e = future_to_range.get(future, (0, 0))
                current_comparison += _pair_range_count(s, e, n)
                if progress_callback:
                    progress_callback(current_comparison, total_comparisons)

        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        self.close()
        return potential_matches
