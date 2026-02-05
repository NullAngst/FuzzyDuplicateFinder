import sys
import os
import cv2
import math
import threading
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QLabel, QFileDialog, 
                             QListWidget, QListWidgetItem, QSplitter, QMessageBox, 
                             QProgressBar, QFrame, QSizePolicy)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QPixmap, QImage
from send2trash import send2trash
from PIL import Image

# Import your engines
from scanner_engine import Scanner
from matcher import Matcher

def format_size(size_bytes):
    if size_bytes == 0: return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

class ScanAndMatchWorker(QThread):
    """
    Runs BOTH the Scanner and the Matcher in a background thread.
    """
    progress_update = pyqtSignal(str)
    finished = pyqtSignal(list)
    error = pyqtSignal(str)

    def __init__(self, target_dir):
        super().__init__()
        self.target_dir = target_dir

    def run(self):
        try:
            # 1. SCAN
            self.progress_update.emit("Phase 1/2: Scanning files...")
            scanner = Scanner()
            db_path = scanner.scan_directory(self.target_dir)

            if not db_path or not os.path.exists(db_path):
                self.error.emit("Database creation failed.")
                return

            # 2. MATCH
            self.progress_update.emit("Phase 2/2: Analyzing duplicates (this may take a moment)...")
            matcher = Matcher(db_path)
            
            # Run the matching algorithms
            exact = matcher.find_exact_duplicates()
            fuzzy = matcher.find_fuzzy_matches()
            
            # 3. COMPILE RESULTS
            final_matches = []

            # Process Exact Matches
            for group in exact:
                base = group[0]
                for duplicate in group[1:]:
                    final_matches.append({
                        'file_a': base['path'],
                        'file_b': duplicate['path'],
                        'score': 100.0,
                        'type': 'EXACT'
                    })

            # Process Fuzzy Matches
            # FIX: Iterate through fuzzy matches and add the 'type' key
            for f in fuzzy:
                f['type'] = 'FUZZY'
                final_matches.append(f)
            
            # Sort by score
            final_matches.sort(key=lambda x: x['score'], reverse=True)

            self.finished.emit(final_matches)

        except Exception as e:
            self.error.emit(str(e))

class DuplicateFinderApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Fuzzy Duplicate Finder")
        self.resize(1400, 900) 

        self.matches = []
        self.current_match_index = -1

        # --- MAIN LAYOUT ---
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # 1. TOP TOOLBAR
        toolbar_frame = QFrame()
        toolbar_frame.setStyleSheet("background-color: #2b2b2b; border-bottom: 1px solid #3e3e3e;")
        toolbar_layout = QHBoxLayout(toolbar_frame)
        
        self.btn_scan = QPushButton("  Select Folder & Scan  ")
        self.btn_scan.setStyleSheet("""
            QPushButton { background-color: #007acc; color: white; border: none; padding: 8px 16px; font-weight: bold; border-radius: 4px; }
            QPushButton:hover { background-color: #005a9e; }
            QPushButton:disabled { background-color: #444; color: #888; }
        """)
        self.btn_scan.clicked.connect(self.start_scan)
        
        self.lbl_status = QLabel("Ready")
        self.lbl_status.setStyleSheet("color: #aaa; margin-left: 10px;")
        
        toolbar_layout.addWidget(self.btn_scan)
        toolbar_layout.addWidget(self.lbl_status)
        toolbar_layout.addStretch()
        
        main_layout.addWidget(toolbar_frame)

        # Progress Bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedHeight(5)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setStyleSheet("QProgressBar { background: #222; border: none; } QProgressBar::chunk { background: #007acc; }")
        self.progress_bar.hide()
        main_layout.addWidget(self.progress_bar)

        # 2. MAIN SPLITTER
        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setHandleWidth(2)
        
        # LEFT: Match List
        self.match_list = QListWidget()
        self.match_list.setFrameShape(QFrame.Shape.NoFrame)
        self.match_list.setStyleSheet("""
            QListWidget { background-color: #222; color: #ddd; font-size: 13px; }
            QListWidget::item { padding: 10px; border-bottom: 1px solid #333; }
            QListWidget::item:selected { background-color: #383838; border-left: 3px solid #007acc; color: white; }
        """)
        self.match_list.currentRowChanged.connect(self.load_match_details)
        splitter.addWidget(self.match_list)

        # RIGHT: Comparison Area
        comparison_widget = QWidget()
        comparison_widget.setStyleSheet("background-color: #1e1e1e;")
        comp_layout = QVBoxLayout(comparison_widget)
        
        preview_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.panel_a = self.create_file_panel("Original / File A")
        self.panel_b = self.create_file_panel("Duplicate / File B")
        preview_splitter.addWidget(self.panel_a['container'])
        preview_splitter.addWidget(self.panel_b['container'])
        comp_layout.addWidget(preview_splitter, stretch=1)
        
        # Action Bar
        action_frame = QFrame()
        action_frame.setStyleSheet("background-color: #252525; border-top: 1px solid #3e3e3e;")
        action_layout = QHBoxLayout(action_frame)
        
        self.lbl_score = QLabel("0%")
        self.lbl_score.setStyleSheet("font-size: 28px; font-weight: bold; color: #4caf50; margin-right: 20px;")
        
        btn_del_a = QPushButton("Delete File A")
        btn_del_a.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_del_a.setStyleSheet("background-color: #d32f2f; color: white; padding: 10px 20px; font-weight: bold; border-radius: 4px;")
        btn_del_a.clicked.connect(lambda: self.delete_file("A"))
        
        btn_keep = QPushButton("Skip / Keep Both")
        btn_keep.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_keep.setStyleSheet("background-color: #555; color: white; padding: 10px 20px; font-weight: bold; border-radius: 4px;")
        btn_keep.clicked.connect(self.next_match)

        btn_del_b = QPushButton("Delete File B")
        btn_del_b.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_del_b.setStyleSheet("background-color: #d32f2f; color: white; padding: 10px 20px; font-weight: bold; border-radius: 4px;")
        btn_del_b.clicked.connect(lambda: self.delete_file("B"))

        action_layout.addStretch()
        action_layout.addWidget(btn_del_a)
        action_layout.addSpacing(20)
        action_layout.addWidget(self.lbl_score)
        action_layout.addWidget(btn_keep)
        action_layout.addSpacing(20)
        action_layout.addWidget(btn_del_b)
        action_layout.addStretch()

        comp_layout.addWidget(action_frame)
        splitter.addWidget(comparison_widget)
        splitter.setSizes([300, 900])
        main_layout.addWidget(splitter)

    def create_file_panel(self, title):
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(10, 10, 10, 10)
        
        lbl_title = QLabel(title)
        lbl_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_title.setStyleSheet("font-weight: bold; color: #888; margin-bottom: 5px;")
        layout.addWidget(lbl_title)
        
        lbl_img = QLabel()
        lbl_img.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_img.setStyleSheet("background-color: #111; border: 1px solid #333; border-radius: 4px;")
        lbl_img.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        lbl_img.setMinimumSize(200, 200)
        layout.addWidget(lbl_img)
        
        meta_frame = QFrame()
        meta_frame.setStyleSheet("background-color: #2b2b2b; border-radius: 4px; margin-top: 10px;")
        meta_layout = QVBoxLayout(meta_frame)
        
        lbl_filename = QLabel("Filename")
        lbl_filename.setStyleSheet("font-size: 14px; font-weight: bold; color: white;")
        lbl_filename.setWordWrap(True)
        
        lbl_path = QLabel("Path")
        lbl_path.setStyleSheet("font-size: 11px; color: #888;")
        lbl_path.setWordWrap(True)
        
        lbl_details = QLabel("Details")
        lbl_details.setStyleSheet("font-size: 12px; color: #ccc; margin-top: 4px;")
        
        btn_open = QPushButton("Open in Explorer")
        btn_open.setStyleSheet("background: transparent; color: #007acc; text-align: left; padding: 0;")
        btn_open.setCursor(Qt.CursorShape.PointingHandCursor)

        meta_layout.addWidget(lbl_filename)
        meta_layout.addWidget(lbl_path)
        meta_layout.addWidget(lbl_details)
        meta_layout.addWidget(btn_open)
        layout.addWidget(meta_frame)
        
        return {"container": container, "img": lbl_img, "filename": lbl_filename, "path": lbl_path, "details": lbl_details, "btn_open": btn_open, "filepath": None}

    def start_scan(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Directory")
        if folder:
            self.lbl_status.setText(f"Initializing scan for: {folder}")
            self.match_list.clear()
            self.progress_bar.setRange(0, 0)
            self.progress_bar.show()
            self.btn_scan.setEnabled(False)
            
            # Start the Worker Thread
            self.worker = ScanAndMatchWorker(folder)
            self.worker.progress_update.connect(self.update_status)
            self.worker.finished.connect(self.on_process_complete)
            self.worker.error.connect(self.on_error)
            self.worker.start()

    def update_status(self, text):
        self.lbl_status.setText(text)

    def on_process_complete(self, matches):
        self.progress_bar.hide()
        self.btn_scan.setEnabled(True)
        self.matches = matches
        
        self.lbl_status.setText(f"Process Complete. Found {len(matches)} duplicates.")
        
        # Populate List
        for m in self.matches:
            name_a = os.path.basename(m['file_a'])
            # Add icon or text to indicate Exact vs Fuzzy
            prefix = "[=]" if m['type'] == 'EXACT' else f"[{int(m['score'])}%]"
            item = QListWidgetItem(f"{prefix} {name_a}")
            self.match_list.addItem(item)

        if len(self.matches) > 0:
            self.match_list.setCurrentRow(0)
        else:
            QMessageBox.information(self, "Clean!", "No duplicates found.")

    def on_error(self, message):
        self.progress_bar.hide()
        self.btn_scan.setEnabled(True)
        QMessageBox.critical(self, "Error", message)
        self.lbl_status.setText("Error occurred.")

    def load_match_details(self, row_index):
        if row_index < 0 or row_index >= len(self.matches): return
        
        data = self.matches[row_index]
        self.current_match_index = row_index
        
        score_text = "Exact Match" if data['type'] == 'EXACT' else f"{int(data['score'])}% Match"
        self.lbl_score.setText(score_text)
        
        self.load_file_to_panel(self.panel_a, data['file_a'])
        self.load_file_to_panel(self.panel_b, data['file_b'])

    def load_file_to_panel(self, panel, filepath):
        panel['filepath'] = filepath
        
        if not os.path.exists(filepath):
            panel['filename'].setText("File Not Found")
            panel['img'].setText("Missing")
            return

        filename = os.path.basename(filepath)
        stats = os.stat(filepath)
        size_str = format_size(stats.st_size)
        ext = os.path.splitext(filepath)[1].lower()
        
        panel['filename'].setText(filename)
        panel['path'].setText(os.path.dirname(filepath))
        
        try: panel['btn_open'].clicked.disconnect() 
        except: pass
        panel['btn_open'].clicked.connect(lambda: os.startfile(filepath) if os.name == 'nt' else os.system(f"open '{filepath}'"))

        res_str = ""
        bitrate_str = ""
        
        # --- PREVIEW GENERATION ---
        # 1. IMAGES
        if ext in ['.jpg', '.png', '.jpeg', '.bmp', '.gif', '.webp']:
            try:
                with Image.open(filepath) as im:
                    res_str = f"{im.width}x{im.height}"
                
                pixmap = QPixmap(filepath)
                if not pixmap.isNull():
                    w = panel['img'].width()
                    h = panel['img'].height()
                    panel['img'].setPixmap(pixmap.scaled(w, h, Qt.AspectRatioMode.KeepAspectRatio))
                else:
                    panel['img'].setText("Image Error")
            except:
                panel['img'].setText("Image Error")
                
        # 2. VIDEO
        elif ext in ['.mp4', '.avi', '.mkv', '.mov', '.wmv']:
            try:
                cap = cv2.VideoCapture(filepath)
                if cap.isOpened():
                    w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                    h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                    res_str = f"{w}x{h}"
                    
                    fps = cap.get(cv2.CAP_PROP_FPS)
                    frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
                    if fps > 0 and frame_count > 0:
                        duration_sec = frame_count / fps
                        bitrate_bps = (stats.st_size * 8) / duration_sec
                        bitrate_str = f"{int(bitrate_bps/1000)} kbps"

                    # Thumbnail
                    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_count // 3) 
                    ret, frame = cap.read()
                    if ret:
                        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        h_img, w_img, ch = frame.shape
                        qimg = QImage(frame.data, w_img, h_img, ch * w_img, QImage.Format.Format_RGB888)
                        panel['img'].setPixmap(QPixmap.fromImage(qimg).scaled(panel['img'].size(), Qt.AspectRatioMode.KeepAspectRatio))
                    else:
                        panel['img'].setText("No Preview")
                    cap.release()
                else:
                    panel['img'].setText("Video File")
            except:
                panel['img'].setText("Video Error")
        
        # 3. AUDIO
        elif ext in ['.mp3', '.wav', '.flac', '.m4a']:
            panel['img'].setText("🎵 Audio File")
            panel['img'].setStyleSheet("background-color: #222; border: 1px solid #333; color: #555; font-size: 20px;")
        
        # 4. OTHER
        else:
            panel['img'].setText(f"📄 {ext} File")
            panel['img'].setStyleSheet("background-color: #222; border: 1px solid #333; color: #555; font-size: 20px;")

        details = f"Size: {size_str}"
        if res_str: details += f"  |  Res: {res_str}"
        if bitrate_str: details += f"  |  Rate: {bitrate_str}"
        panel['details'].setText(details)

    def delete_file(self, target):
        if self.current_match_index == -1: return
        
        panel = self.panel_a if target == "A" else self.panel_b
        filepath = panel['filepath']

        confirm = QMessageBox.question(self, "Confirm Delete", 
                                       f"Are you sure you want to send this to Trash?\n\n{filepath}",
                                       QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        
        if confirm == QMessageBox.StandardButton.Yes:
            try:
                send2trash(filepath)
                self.lbl_status.setText(f"Deleted {os.path.basename(filepath)}")
                self.next_match()
            except Exception as e:
                QMessageBox.critical(self, "Error", str(e))

    def next_match(self):
        current_row = self.match_list.currentRow()
        if current_row < self.match_list.count() - 1:
            self.match_list.setCurrentRow(current_row + 1)
        else:
            QMessageBox.information(self, "Done", "No more matches!")

    def resizeEvent(self, event):
        if self.current_match_index != -1:
             self.load_match_details(self.current_match_index)
        super().resizeEvent(event)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = DuplicateFinderApp()
    window.show()
    sys.exit(app.exec())