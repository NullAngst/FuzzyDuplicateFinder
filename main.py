import sys
import os
import cv2
import threading
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QLabel, QFileDialog, 
                             QListWidget, QListWidgetItem, QSplitter, QMessageBox, 
                             QProgressBar, QFrame, QSizePolicy)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QPixmap, QImage, QAction
from send2trash import send2trash

# Visualization libs
import matplotlib
matplotlib.use('Agg') # Use non-interactive backend
import matplotlib.pyplot as plt
from io import BytesIO
try:
    import librosa
    import librosa.display
    import numpy as np
except ImportError:
    pass

# Import engines
from scanner_engine import Scanner
from matcher import Matcher

class WorkerThread(QThread):
    """
    Runs the heavy scanning logic in a background thread.
    Returns the path of the created database upon completion.
    """
    progress_update = pyqtSignal(str)
    finished = pyqtSignal(str) # Returns path to DB

    def __init__(self, target_dir):
        super().__init__()
        self.target_dir = target_dir

    def run(self):
        self.progress_update.emit("Scanning directory...")
        scanner = Scanner()
        # db_output_path=None forces it to save inside the scanned folder
        db_path = scanner.scan_directory(self.target_dir, db_output_path=None)
        
        self.progress_update.emit("Scan complete. Analyzing matches...")
        self.finished.emit(db_path)

class DuplicateFinderApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Insane Duplicate Finder")
        self.resize(1300, 900)

        # State
        self.matches = []
        self.current_match_index = -1
        self.current_db_path = None # Track where our DB is

        # --- UI LAYOUT ---
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # Toolbar
        header_layout = QHBoxLayout()
        self.btn_scan = QPushButton("Select Folder & Scan")
        self.btn_scan.clicked.connect(self.start_scan)
        
        self.btn_cleanup = QPushButton("Delete DB & Exit")
        self.btn_cleanup.setStyleSheet("background-color: #554444;")
        self.btn_cleanup.clicked.connect(self.cleanup_db)
        self.btn_cleanup.setEnabled(False)

        self.lbl_status = QLabel("Ready")
        
        header_layout.addWidget(self.btn_scan)
        header_layout.addWidget(self.btn_cleanup)
        header_layout.addWidget(self.lbl_status)
        main_layout.addLayout(header_layout)

        # Progress Bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)
        self.progress_bar.hide()
        main_layout.addWidget(self.progress_bar)

        # Splitter
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left: List
        self.match_list = QListWidget()
        self.match_list.currentRowChanged.connect(self.load_match_details)
        splitter.addWidget(self.match_list)

        # Right: Comparison Container
        self.comparison_container = QWidget()
        comp_layout = QHBoxLayout(self.comparison_container)
        
        self.panel_a = self.create_file_panel("File A")
        self.panel_actions = self.create_action_panel()
        self.panel_b = self.create_file_panel("File B")

        # FIX: Access the 'layout' key from the dictionary
        comp_layout.addLayout(self.panel_a['layout']) 
        comp_layout.addLayout(self.panel_actions)     
        comp_layout.addLayout(self.panel_b['layout']) 
        
        splitter.addWidget(self.comparison_container)
        splitter.setStretchFactor(1, 4)

        main_layout.addWidget(splitter)
        self.apply_styles()

    def create_file_panel(self, title):
        layout = QVBoxLayout()
        lbl_title = QLabel(title)
        lbl_title.setStyleSheet("font-weight: bold; font-size: 14px;")
        
        lbl_img = QLabel()
        lbl_img.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_img.setStyleSheet("background-color: #222; border: 1px solid #444;")
        lbl_img.setMinimumSize(300, 300)
        
        lbl_info = QLabel("No File")
        lbl_info.setWordWrap(True)
        
        btn_open = QPushButton("Open File")
        
        layout.addWidget(lbl_title)
        layout.addWidget(lbl_img)
        layout.addWidget(lbl_info)
        layout.addWidget(btn_open)
        
        return {
            "layout": layout,
            "img": lbl_img,
            "info": lbl_info,
            "btn_open": btn_open,
            "path": None
        }

    def create_action_panel(self):
        layout = QVBoxLayout()
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.lbl_score = QLabel("0%")
        self.lbl_score.setStyleSheet("font-size: 24px; font-weight: bold; color: #00ff00;")
        
        btn_del_a = QPushButton("Delete A")
        btn_del_a.setStyleSheet("background-color: #aa0000; color: white; font-weight: bold;")
        btn_del_a.clicked.connect(lambda: self.delete_file("A"))
        
        btn_del_b = QPushButton("Delete B")
        btn_del_b.setStyleSheet("background-color: #aa0000; color: white; font-weight: bold;")
        btn_del_b.clicked.connect(lambda: self.delete_file("B"))
        
        btn_skip = QPushButton("Skip / Keep Both")
        btn_skip.clicked.connect(self.next_match)

        layout.addWidget(QLabel("Similarity"))
        layout.addWidget(self.lbl_score)
        layout.addSpacing(20)
        layout.addWidget(btn_del_a)
        layout.addWidget(btn_del_b)
        layout.addSpacing(20)
        layout.addWidget(btn_skip)
        
        return layout

    def start_scan(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Directory")
        if folder:
            self.lbl_status.setText(f"Scanning: {folder}")
            self.match_list.clear()
            self.progress_bar.show()
            self.btn_scan.setEnabled(False)
            self.btn_cleanup.setEnabled(False)
            
            self.worker = WorkerThread(folder)
            self.worker.progress_update.connect(lambda s: self.lbl_status.setText(s))
            self.worker.finished.connect(self.on_scan_finished)
            self.worker.start()

    def on_scan_finished(self, db_path):
        self.progress_bar.hide()
        self.btn_scan.setEnabled(True)
        self.btn_cleanup.setEnabled(True)
        self.lbl_status.setText("Scan Complete. Loading matches...")
        self.current_db_path = db_path # Save for cleanup later
        self.load_matches(db_path)

    def load_matches(self, db_path):
        if not os.path.exists(db_path):
            self.lbl_status.setText("Error: Database not found.")
            return

        matcher = Matcher(db_path)
        
        # 1. Exact Matches
        exact = matcher.find_exact_duplicates()
        # 2. Fuzzy Matches
        fuzzy = matcher.find_fuzzy_matches()
        
        self.matches = []
        
        # Format Exact
        for group in exact:
            base = group[0]
            for duplicate in group[1:]:
                self.matches.append({
                    'file_a': base['path'],
                    'file_b': duplicate['path'],
                    'score': 100.0,
                    'type': 'Exact MD5'
                })

        # Add Fuzzy
        self.matches.extend(fuzzy)
        self.matches.sort(key=lambda x: x['score'], reverse=True)

        for m in self.matches:
            name_a = os.path.basename(m['file_a'])
            name_b = os.path.basename(m['file_b'])
            item = QListWidgetItem(f"[{m['score']}%] {name_a} vs {name_b}")
            self.match_list.addItem(item)

        self.lbl_status.setText(f"Found {len(self.matches)} potential duplicates.")

    def load_match_details(self, row_index):
        if row_index < 0 or row_index >= len(self.matches): return
        data = self.matches[row_index]
        self.current_match_index = row_index
        
        self.lbl_score.setText(f"{int(data['score'])}%")
        self.load_file_into_panel(self.panel_a, data['file_a'])
        self.load_file_into_panel(self.panel_b, data['file_b'])

    def generate_waveform(self, filepath):
        """Generates a waveform image for audio files."""
        try:
            # Load 30 seconds max for speed
            y, sr = librosa.load(filepath, duration=30)
            plt.figure(figsize=(4, 3), facecolor="#222222")
            ax = plt.gca()
            ax.set_facecolor("#222222")
            librosa.display.waveshow(y, sr=sr, color="#007acc")
            plt.axis('off')
            plt.tight_layout()
            
            # Save to buffer
            buf = BytesIO()
            plt.savefig(buf, format='png', bbox_inches='tight', pad_inches=0)
            buf.seek(0)
            plt.close()
            
            qimg = QImage.fromData(buf.read())
            return QPixmap.fromImage(qimg)
        except Exception as e:
            print(f"Waveform Error: {e}")
            return None

    def load_file_into_panel(self, panel, filepath):
        panel['path'] = filepath
        
        if not os.path.exists(filepath):
            panel['info'].setText("File deleted or missing.")
            panel['img'].clear()
            panel['img'].setText("Missing")
            return

        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        ext = os.path.splitext(filepath)[1].lower()
        info = f"{os.path.basename(filepath)}\n{size_mb:.2f} MB\n{ext}"
        panel['info'].setText(info)
        
        # Connect Open Button
        try: panel['btn_open'].clicked.disconnect() 
        except: pass
        panel['btn_open'].clicked.connect(lambda: os.startfile(filepath) if os.name == 'nt' else os.system(f"open '{filepath}'"))

        # --- PREVIEW LOGIC ---
        if ext in ['.jpg', '.png', '.jpeg', '.bmp']:
            pixmap = QPixmap(filepath)
            if not pixmap.isNull():
                panel['img'].setPixmap(pixmap.scaled(300, 300, Qt.AspectRatioMode.KeepAspectRatio))
        
        elif ext in ['.mp4', '.avi', '.mkv']:
            try:
                cap = cv2.VideoCapture(filepath)
                ret, frame = cap.read()
                cap.release()
                if ret:
                    frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    h, w, ch = frame.shape
                    qimg = QImage(frame.data, w, h, ch * w, QImage.Format.Format_RGB888)
                    panel['img'].setPixmap(QPixmap.fromImage(qimg).scaled(300, 300, Qt.AspectRatioMode.KeepAspectRatio))
            except:
                panel['img'].setText("Video File")
        
        elif ext in ['.mp3', '.wav', '.flac']:
            # Generate Audio Waveform
            panel['img'].setText("Generating Waveform...")
            # We process this in main thread for simplicity, 
            # though ideally this should be threaded for large files.
            waveform = self.generate_waveform(filepath)
            if waveform:
                panel['img'].setPixmap(waveform)
            else:
                panel['img'].setText("Audio File (No Preview)")
        
        else:
            panel['img'].setText(f"{ext} File")

    def delete_file(self, target):
        if self.current_match_index == -1: return
        panel = self.panel_a if target == "A" else self.panel_b
        filepath = panel['path']

        confirm = QMessageBox.question(self, "Confirm Delete", 
                                       f"Send to Trash?\n{filepath}",
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

    def cleanup_db(self):
        """Deletes the database file to leave the folder clean."""
        if self.current_db_path and os.path.exists(self.current_db_path):
            confirm = QMessageBox.question(self, "Clean Up", 
                                           "This will delete the 'duplicate_index.db' file from the folder.\nScan results will be lost.\n\nContinue?",
                                           QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if confirm == QMessageBox.StandardButton.Yes:
                try:
                    # Close connections in matcher? 
                    # Actually, matcher recreates connection every time, but just to be safe:
                    # We can't easily force close the matcher's pointer here without restructuring,
                    # but usually it's fine if the scan is done.
                    os.remove(self.current_db_path)
                    QMessageBox.information(self, "Done", "Database deleted.")
                    self.close()
                except Exception as e:
                    QMessageBox.critical(self, "Error", f"Could not delete DB: {e}")

    def apply_styles(self):
        self.setStyleSheet("""
            QMainWindow { background-color: #333; color: #fff; }
            QLabel { color: #eee; }
            QListWidget { background-color: #222; color: #fff; border: 1px solid #555; font-size: 14px; }
            QListWidget::item:selected { background-color: #007acc; }
            QPushButton { padding: 8px; background-color: #555; color: #fff; border-radius: 4px; }
            QPushButton:hover { background-color: #666; }
        """)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = DuplicateFinderApp()
    window.show()
    sys.exit(app.exec())