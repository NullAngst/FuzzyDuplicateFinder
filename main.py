import sys
import os
import cv2
import math
import shutil
import time
from datetime import datetime
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QLabel, QFileDialog, 
                             QListWidget, QListWidgetItem, QSplitter, QMessageBox, 
                             QProgressBar, QFrame, QSizePolicy, QMenu, QTableWidget, 
                             QTableWidgetItem, QHeaderView, QAbstractItemView, QSpinBox,
                             QDialog, QTextEdit, QProgressDialog)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QPixmap, QImage, QAction
from PyQt6.QtCore import QUrl
from PyQt6.QtGui import QDesktopServices
from send2trash import send2trash
from PIL import Image

from scanner_engine import Scanner, DatabaseManager
from matcher import Matcher

# --- VERSION INFO ---
VERSION = "1.3.0"
GITHUB_URL = "https://github.com/MZGSZM/FuzzyDuplicateFinder"

def format_size(size_bytes):
    if size_bytes == 0: return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

class SkippedFileDialog(QDialog):
    def __init__(self, skipped_files, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Skipped Files")
        self.resize(600, 400)
        self.skipped_files = skipped_files
        
        layout = QVBoxLayout(self)
        
        lbl = QLabel(f"{len(skipped_files)} files could not be processed (Permission denied, corrupted, or unsupported):")
        layout.addWidget(lbl)
        
        self.list_widget = QListWidget()
        self.list_widget.addItems(skipped_files)
        layout.addWidget(self.list_widget)
        
        btn_layout = QHBoxLayout()
        btn_export = QPushButton("Export List to TXT")
        btn_export.clicked.connect(self.export_list)
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(self.close)
        
        btn_layout.addStretch()
        btn_layout.addWidget(btn_export)
        btn_layout.addWidget(btn_close)
        layout.addLayout(btn_layout)
        
        self.setStyleSheet("""
            QDialog { background-color: #333; color: #eee; }
            QListWidget { background-color: #222; color: #ccc; border: 1px solid #444; }
            QPushButton { background-color: #444; color: white; padding: 6px 12px; border-radius: 4px; }
            QPushButton:hover { background-color: #555; }
        """)

    def export_list(self):
        path, _ = QFileDialog.getSaveFileName(self, "Export Skipped Files", "skipped_files.txt", "Text Files (*.txt)")
        if path:
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write("\n".join(self.skipped_files))
                QMessageBox.information(self, "Export Successful", f"Saved to {path}")
            except Exception as e:
                QMessageBox.critical(self, "Error", str(e))

class ScanAndMatchWorker(QThread):
    progress_update = pyqtSignal(str)
    progress_value = pyqtSignal(int, int) # current, total
    scan_complete = pyqtSignal(list) # Emits skipped files
    finished = pyqtSignal(list)
    error = pyqtSignal(str)
    aborted = pyqtSignal()

    def __init__(self, folder_list, db_path, skip_scan=False):
        super().__init__()
        self.folder_list = folder_list 
        self.db_path = db_path
        self.skip_scan = skip_scan
        self._is_running = True

    def stop(self):
        self._is_running = False

    def is_stopped(self):
        return not self._is_running

    def on_scan_progress(self, current, total, skipped):
        if self._is_running:
            self.progress_value.emit(current, total)
            self.progress_update.emit(f"Scanning: {current} / {total} files (Skipped: {skipped})")

    def on_match_progress(self, current, total):
        if self._is_running:
            self.progress_value.emit(current, total)

    def run(self):
        try:
            if not self.skip_scan:
                self.progress_update.emit("Phase 1: Indexing files...")
                scanner = Scanner()
                # Unpack tuple (db_path, skipped_list)
                result_db, skipped_list = scanner.scan_directory(
                    self.folder_list, 
                    self.db_path, 
                    stop_signal=self.is_stopped,
                    progress_callback=self.on_scan_progress
                )
                
                if self.is_stopped(): self.aborted.emit(); return
                if not result_db: self.error.emit("DB Failed"); return
                
                # Update DB path just in case
                self.db_path = result_db 
                self.scan_complete.emit(skipped_list)
            else:
                self.progress_update.emit("Skipping scan. Loading index...")
                self.scan_complete.emit([]) # No new skips if we skipped scan

            self.progress_update.emit("Phase 2: Analyzing content...")
            matcher = Matcher(self.db_path)
            
            exact = matcher.find_exact_duplicates()
            if self.is_stopped(): matcher.close(); self.aborted.emit(); return

            fuzzy = matcher.find_fuzzy_matches(
                stop_signal=self.is_stopped,
                progress_callback=self.on_match_progress 
            )
            
            if self.is_stopped(): matcher.close(); self.aborted.emit(); return
            
            final_matches = []
            for group in exact:
                base = group[0]
                for duplicate in group[1:]:
                    final_matches.append({
                        'file_a': base['path'], 'file_b': duplicate['path'],
                        'score': 100.0, 'type': 'EXACT'
                    })

            for f in fuzzy:
                f['type'] = 'FUZZY'
                final_matches.append(f)
            
            final_matches.sort(key=lambda x: x['score'], reverse=True)
            self.finished.emit(final_matches)

        except Exception as e:
            if not self.is_stopped(): self.error.emit(str(e))

class AutoPruneWorker(QThread):
    progress_update = pyqtSignal(str)
    progress_value = pyqtSignal(int, int)  # current, total
    finished = pyqtSignal(int)  # deleted count
    error = pyqtSignal(str)
    aborted = pyqtSignal()

    def __init__(self, files_to_trash):
        super().__init__()
        self.files_to_trash = list(files_to_trash)
        self._is_running = True

    def stop(self):
        self._is_running = False

    def is_stopped(self):
        return not self._is_running

    def run(self):
        try:
            deleted_count = 0
            total = len(self.files_to_trash)
            
            for i, filepath in enumerate(self.files_to_trash):
                if self.is_stopped():
                    self.aborted.emit()
                    return
                
                try:
                    if os.path.exists(filepath):
                        send2trash(filepath)
                        deleted_count += 1
                except Exception as e:
                    print(f"Failed to delete {filepath}: {e}")
                
                self.progress_value.emit(i + 1, total)
                self.progress_update.emit(f"Pruning: {i + 1} / {total} files deleted")
            
            self.finished.emit(deleted_count)
        except Exception as e:
            if not self.is_stopped():
                self.error.emit(str(e))

class DuplicateFinderApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Fuzzy Duplicate Finder")
        self.resize(1400, 950) 

        self.scan_folders = [] 
        self.matches = []
        self.skipped_files = [] # Store skipped files here
        self.current_match_index = -1
        self.current_db_path = None
        self.worker = None
        self.prune_worker = None
        self.prune_progress_dialog = None
        self.pixmap_cache = {'A': None, 'B': None}

        # --- MENU BAR ---
        menubar = self.menuBar()
        file_menu = menubar.addMenu("File")
        exit_action = QAction("Exit", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        tools_menu = menubar.addMenu("Tools")
        prune_exact_action = QAction("Auto-Prune Exact Duplicates...", self)
        prune_exact_action.triggered.connect(self.auto_prune_exact)
        tools_menu.addAction(prune_exact_action)

        # --- STATUS BAR ---
        self.status_bar = self.statusBar()
        self.lbl_status = QLabel("Ready")
        self.lbl_status.setStyleSheet("color: #aaa; margin-left: 10px; font-weight: bold;")
        self.status_bar.addWidget(self.lbl_status)
        
        # Version label (clickable)
        self.lbl_version = QLabel(f"v{VERSION}")
        self.lbl_version.setStyleSheet("color: #666; font-size: 10px; margin-right: 10px;")
        self.lbl_version.setCursor(Qt.CursorShape.PointingHandCursor)
        self.lbl_version.mousePressEvent = lambda event: self.open_github()
        self.status_bar.addPermanentWidget(self.lbl_version)

        # --- MAIN LAYOUT (VERTICAL SPLITTER) ---
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(5, 5, 5, 5)
        
        # Vertical Splitter: Top (Controls) vs Bottom (Comparisons)
        v_splitter = QSplitter(Qt.Orientation.Vertical)
        
        # --- TOP PANEL ---
        top_container = QWidget()
        top_layout = QVBoxLayout(top_container)
        top_layout.setContentsMargins(0, 0, 0, 0)

        # Button Row
        btn_row = QHBoxLayout()
        btn_add_folder = QPushButton(" + Add Folder ")
        btn_add_folder.clicked.connect(self.add_folder)
        self.style_button(btn_add_folder, bg="#444")

        btn_clear_folders = QPushButton(" Clear List ")
        btn_clear_folders.clicked.connect(self.clear_folders)
        self.style_button(btn_clear_folders, bg="#444")
        
        btn_load_index = QPushButton(" Load Index... ")
        btn_load_index.clicked.connect(self.load_index)
        self.style_button(btn_load_index, bg="#444")

        self.btn_scan = QPushButton("  START SCAN  ")
        self.btn_scan.setEnabled(False)
        self.btn_scan.clicked.connect(self.start_scan)
        self.style_button(self.btn_scan, bg="#007acc")

        self.btn_stop = QPushButton("  STOP  ")
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self.stop_scan)
        self.style_button(self.btn_stop, bg="#d32f2f")
        
        # New Skipped Files Button
        self.btn_skipped = QPushButton("0 Skipped")
        self.btn_skipped.setStyleSheet("""
            QPushButton { background: transparent; color: #d32f2f; text-decoration: underline; border: none; font-weight: bold; text-align: left; }
            QPushButton:hover { color: #ff6659; }
        """)
        self.btn_skipped.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_skipped.clicked.connect(self.show_skipped_dialog)
        self.btn_skipped.hide()

        btn_row.addWidget(btn_add_folder)
        btn_row.addWidget(btn_clear_folders)
        btn_row.addWidget(btn_load_index)
        btn_row.addSpacing(20)
        btn_row.addWidget(self.btn_scan)
        btn_row.addWidget(self.btn_stop)
        btn_row.addWidget(self.btn_skipped)
        btn_row.addStretch()

        # Folder List Table
        self.folder_table = QTableWidget()
        self.folder_table.setColumnCount(2)
        self.folder_table.setHorizontalHeaderLabels(["Folder Path", "Priority"])
        self.folder_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.folder_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Fixed)
        self.folder_table.setColumnWidth(1, 80)
        self.folder_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.folder_table.setStyleSheet("QTableWidget { background-color: #222; color: #eee; border: 1px solid #444; } QHeaderView::section { background-color: #333; color: #ddd; }")
        
        top_layout.addLayout(btn_row)
        top_layout.addWidget(self.folder_table)
        
        # Progress Bar (Between Top and Bottom)
        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedHeight(5)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setStyleSheet("QProgressBar { background: #222; border: none; } QProgressBar::chunk { background: #007acc; }")
        self.progress_bar.hide()
        top_layout.addWidget(self.progress_bar)

        v_splitter.addWidget(top_container)

        # --- BOTTOM PANEL (Comparison) ---
        bottom_container = QWidget()
        bottom_layout = QHBoxLayout(bottom_container)
        bottom_layout.setContentsMargins(0, 0, 0, 0)

        # Horizontal Splitter: List vs Preview
        h_splitter = QSplitter(Qt.Orientation.Horizontal)
        h_splitter.setHandleWidth(2)
        
        self.match_list = QListWidget()
        self.match_list.setFrameShape(QFrame.Shape.NoFrame)
        self.match_list.setStyleSheet("""
            QListWidget { background-color: #222; color: #ddd; font-size: 13px; }
            QListWidget::item { padding: 8px; border-bottom: 1px solid #333; }
            QListWidget::item:selected { background-color: #383838; border-left: 3px solid #007acc; color: white; }
        """)
        self.match_list.currentRowChanged.connect(self.load_match_details)
        h_splitter.addWidget(self.match_list)

        # Comparison Area
        comparison_widget = QWidget()
        comparison_widget.setStyleSheet("background-color: #1e1e1e;")
        comp_layout = QVBoxLayout(comparison_widget)
        
        # Preview Splitter
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
        btn_del_a.clicked.connect(lambda: self.delete_file("A"))
        self.style_button(btn_del_a, bg="#d32f2f")
        
        btn_del_both = QPushButton("Delete Both Files")
        btn_del_both.clicked.connect(self.delete_both_files)
        self.style_button(btn_del_both, bg="#d32f2f")

        btn_keep = QPushButton("Skip / Keep Both")
        btn_keep.clicked.connect(self.next_match)
        self.style_button(btn_keep, bg="#555")

        btn_del_b = QPushButton("Delete File B")
        btn_del_b.clicked.connect(lambda: self.delete_file("B"))
        self.style_button(btn_del_b, bg="#d32f2f")

        action_layout.addStretch()
        action_layout.addWidget(btn_del_a)
        action_layout.addSpacing(20)
        action_layout.addWidget(btn_del_both)
        action_layout.addSpacing(20)
        action_layout.addWidget(self.lbl_score)
        action_layout.addWidget(btn_keep)
        action_layout.addSpacing(20)
        action_layout.addWidget(btn_del_b)
        action_layout.addStretch()

        comp_layout.addWidget(action_frame)
        
        h_splitter.addWidget(comparison_widget)
        h_splitter.setSizes([350, 900]) # Initial List vs Preview widths
        
        bottom_layout.addWidget(h_splitter)
        v_splitter.addWidget(bottom_container)
        
        # Set initial vertical sizes (Top small, Bottom big)
        v_splitter.setSizes([200, 700]) 
        
        main_layout.addWidget(v_splitter)

    def style_button(self, btn, bg):
        btn.setCursor(Qt.CursorShape.PointingHandCursor)
        hover_map = { "#d32f2f": "#ef5350", "#007acc": "#2196f3", "#444": "#666", "#555": "#777" }
        hover_color = hover_map.get(bg, bg)
        btn.setStyleSheet(f"QPushButton {{ background-color: {bg}; color: white; padding: 8px 16px; font-weight: bold; border-radius: 4px; border: none; }} QPushButton:hover {{ background-color: {hover_color}; }} QPushButton:pressed {{ background-color: {bg}; }} QPushButton:disabled {{ background-color: #333; color: #555; }}")

    def create_file_panel(self, title):
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(10, 10, 10, 10)
        
        lbl_title = QLabel(title)
        lbl_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_title.setStyleSheet("font-weight: bold; color: #888;")
        layout.addWidget(lbl_title)
        
        lbl_img = QLabel()
        lbl_img.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_img.setStyleSheet("background-color: #111; border: 1px solid #333; border-radius: 4px;")
        
        # --- FIX FOR TINY IMAGES ---
        # Ignored + Stretch Factor 1 forces it to fill available space
        lbl_img.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Ignored)
        lbl_img.setScaledContents(False) 
        layout.addWidget(lbl_img)
        layout.setStretchFactor(lbl_img, 1)
        
        meta_frame = QFrame()
        meta_frame.setStyleSheet("background-color: #2b2b2b; border-radius: 4px; margin-top: 10px;")
        meta_layout = QVBoxLayout(meta_frame)
        
        lbl_filename = QLabel("Filename")
        lbl_filename.setStyleSheet("font-size: 14px; font-weight: bold; color: white;")
        lbl_filename.setWordWrap(True)
        lbl_path = QLabel("Path"); lbl_path.setStyleSheet("font-size: 11px; color: #aaa;"); lbl_path.setWordWrap(True)
        lbl_details = QLabel("Details"); lbl_details.setStyleSheet("font-size: 11px; color: #ccc; margin-top: 4px;")
        lbl_dates = QLabel("Dates"); lbl_dates.setStyleSheet("font-size: 11px; color: #888;")

        btn_open = QPushButton("Open in Viewer")
        btn_open.setStyleSheet("background: transparent; color: #007acc; text-align: left; padding: 0; border: none;")
        btn_open.setCursor(Qt.CursorShape.PointingHandCursor)

        meta_layout.addWidget(lbl_filename); meta_layout.addWidget(lbl_path); meta_layout.addWidget(lbl_details); meta_layout.addWidget(lbl_dates); meta_layout.addWidget(btn_open)
        layout.addWidget(meta_frame)
        return {"container": container, "img": lbl_img, "filename": lbl_filename, "path": lbl_path, "details": lbl_details, "dates": lbl_dates, "btn_open": btn_open, "filepath": None}

    # ... (Rest of the class methods remain the same: add_folder, start_scan, load_index, etc.) ...
    
    def add_folder(self):
        """Add folders with native multi-select when available"""
        folder = QFileDialog.getExistingDirectory(self, "Select Directory")
        if folder:
            for f in self.scan_folders:
                if f['path'] == folder: return
            self.scan_folders.append({'path': folder, 'priority': 10})
            self.refresh_folder_table()
            self.btn_scan.setEnabled(True)

    def clear_folders(self):
        self.scan_folders = []
        self.refresh_folder_table()
        self.btn_scan.setEnabled(False)

    def load_index(self):
        db_path, _ = QFileDialog.getOpenFileName(self, "Load Existing Index", "", "Database Files (*.db)")
        if db_path:
            self.current_db_path = db_path
            db = DatabaseManager(db_path)
            roots = db.get_roots()
            db.close()
            if roots:
                self.scan_folders = roots
                self.refresh_folder_table()  # This refreshes the UI with loaded priorities
            self.start_worker(skip_scan=True)

    def refresh_folder_table(self):
        self.folder_table.setRowCount(len(self.scan_folders))
        for i, folder_data in enumerate(self.scan_folders):
            path_item = QTableWidgetItem(folder_data['path'])
            path_item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
            self.folder_table.setItem(i, 0, path_item)
            
            # Create custom priority widget with arrow buttons
            priority_widget = QWidget()
            priority_layout = QHBoxLayout(priority_widget)
            priority_layout.setContentsMargins(0, 0, 0, 0)
            priority_layout.setSpacing(2)
            
            # Display value
            lbl_value = QLabel(str(folder_data['priority']))
            lbl_value.setAlignment(Qt.AlignmentFlag.AlignCenter)
            lbl_value.setStyleSheet("color: white; font-weight: bold; min-width: 20px;")
            
            # Up arrow button
            btn_up = QPushButton("▲")
            btn_up.setMaximumWidth(20)
            btn_up.setMaximumHeight(16)
            btn_up.setStyleSheet("""
                QPushButton {
                    background-color: #555;
                    color: #fff;
                    font-size: 10px;
                    font-weight: bold;
                    border: 1px solid #333;
                    padding: 0px;
                    border-radius: 2px;
                }
                QPushButton:hover { background-color: #2196f3; }
                QPushButton:pressed { background-color: #1976d2; }
            """)
            
            # Down arrow button
            btn_down = QPushButton("▼")
            btn_down.setMaximumWidth(20)
            btn_down.setMaximumHeight(16)
            btn_down.setStyleSheet("""
                QPushButton {
                    background-color: #555;
                    color: #fff;
                    font-size: 10px;
                    font-weight: bold;
                    border: 1px solid #333;
                    padding: 0px;
                    border-radius: 2px;
                }
                QPushButton:hover { background-color: #2196f3; }
                QPushButton:pressed { background-color: #1976d2; }
            """)
            
            # FIX: Store index and label in button properties
            btn_up.folder_index = i
            btn_up.priority_label = lbl_value
            btn_down.folder_index = i
            btn_down.priority_label = lbl_value
            
            # Connect to methods that read the stored properties
            btn_up.clicked.connect(self._on_priority_up_clicked)
            btn_down.clicked.connect(self._on_priority_down_clicked)
            
            priority_layout.addWidget(btn_down)
            priority_layout.addWidget(lbl_value)
            priority_layout.addWidget(btn_up)
            
            self.folder_table.setCellWidget(i, 1, priority_widget)

    def _on_priority_up_clicked(self):
        """Helper method for up button click"""
        btn = self.sender()
        idx = btn.folder_index
        label = btn.priority_label
        if idx < len(self.scan_folders):
            new_val = min(100, self.scan_folders[idx]['priority'] + 1)
            self.scan_folders[idx]['priority'] = new_val
            label.setText(str(new_val))
            self.persist_folder_priorities()

    def _on_priority_down_clicked(self):
        """Helper method for down button click"""
        btn = self.sender()
        idx = btn.folder_index
        label = btn.priority_label
        if idx < len(self.scan_folders):
            new_val = max(0, self.scan_folders[idx]['priority'] - 1)
            self.scan_folders[idx]['priority'] = new_val
            label.setText(str(new_val))
            self.persist_folder_priorities()

    def update_priority(self, index, value):
        if 0 <= index < len(self.scan_folders):
            self.scan_folders[index]['priority'] = value

    def start_scan(self):
        if not self.scan_folders: return
        if not self.current_db_path:
             if len(self.scan_folders) == 1:
                self.current_db_path = os.path.join(self.scan_folders[0]['path'], "duplicate_index.db")
             else:
                save_path, _ = QFileDialog.getSaveFileName(self, "Save Database Location", "duplicate_index.db", "Database Files (*.db)")
                if save_path: self.current_db_path = save_path
                else: return
        
        # Check if database already exists and ask user to confirm overwrite
        if os.path.exists(self.current_db_path):
            confirm = QMessageBox.question(
                self,
                "Database Exists",
                f"Database already exists at:\n\n{self.current_db_path}\n\nOverwrite and start fresh scan?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if confirm != QMessageBox.StandardButton.Yes:
                return
            
            # Delete existing database and WAL files
            db_files = [
                self.current_db_path,
                self.current_db_path + "-shm",
                self.current_db_path + "-wal"
            ]
            for db_file in db_files:
                try:
                    if os.path.exists(db_file):
                        os.remove(db_file)
                except Exception as e:
                    QMessageBox.warning(self, "Warning", f"Could not delete old database file: {e}")
                    return
        
        self.start_worker(skip_scan=False)

    def start_worker(self, skip_scan=False):
        self.lbl_status.setText("Working...")
        self.match_list.clear()
        self.btn_skipped.hide()
        self.skipped_files = []
        
        if not skip_scan:
            self.progress_bar.setRange(0, 0)
            self.progress_bar.show()
        self.btn_scan.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.worker = ScanAndMatchWorker(self.scan_folders, self.current_db_path, skip_scan=skip_scan)
        self.worker.progress_update.connect(lambda s: self.lbl_status.setText(s))
        self.worker.progress_value.connect(self.update_progress_bar)
        self.worker.scan_complete.connect(self.on_scan_phase_complete)
        self.worker.finished.connect(self.on_process_complete)
        self.worker.aborted.connect(self.on_scan_aborted)
        self.worker.error.connect(self.on_error)
        self.worker.start()

    def update_progress_bar(self, current, total):
        self.progress_bar.show()
        self.progress_bar.setRange(0, total)
        self.progress_bar.setValue(current)

    def on_scan_phase_complete(self, skipped_list):
        self.skipped_files = skipped_list
        if len(skipped_list) > 0:
            self.btn_skipped.setText(f"{len(skipped_list)} Files Skipped (View)")
            self.btn_skipped.show()

    def show_skipped_dialog(self):
        if not self.skipped_files: return
        dlg = SkippedFileDialog(self.skipped_files, self)
        dlg.exec()

    def stop_scan(self):
        if self.worker and self.worker.isRunning():
            self.lbl_status.setText("Stopping... Please wait.")
            self.btn_stop.setEnabled(False)
            self.worker.stop()

    def on_scan_aborted(self):
        self.progress_bar.hide()
        self.btn_scan.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.lbl_status.setText("Operation Aborted.")
        
    def on_process_complete(self, matches):
        self.progress_bar.hide()
        self.btn_scan.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.matches = matches
        self.lbl_status.setText(f"Found {len(matches)} duplicates.")
        for m in self.matches:
            name_a = os.path.basename(m['file_a'])
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
        self.btn_stop.setEnabled(False)
        QMessageBox.critical(self, "Error", message)

    def load_match_details(self, row_index):
        if row_index < 0 or row_index >= len(self.matches): return
        data = self.matches[row_index]
        self.current_match_index = row_index
        score_text = "Exact Match" if data['type'] == 'EXACT' else f"{int(data['score'])}% Match"
        self.lbl_score.setText(score_text)
        self.load_file_to_panel(self.panel_a, data['file_a'], 'A')
        self.load_file_to_panel(self.panel_b, data['file_b'], 'B')

    def load_file_to_panel(self, panel, filepath, cache_key):
        panel['filepath'] = filepath
        self.pixmap_cache[cache_key] = None
        if not os.path.exists(filepath):
            panel['filename'].setText("File Missing")
            panel['path'].setText(filepath)
            panel['img'].setText("Missing on Disk")
            return
        filename = os.path.basename(filepath)
        stats = os.stat(filepath)
        size_str = format_size(stats.st_size)
        ext = os.path.splitext(filepath)[1].lower()
        c_time = datetime.fromtimestamp(stats.st_ctime).strftime('%Y-%m-%d %H:%M')
        m_time = datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M')
        panel['dates'].setText(f"Created: {c_time}  |  Modified: {m_time}")
        panel['filename'].setText(filename)
        panel['path'].setText(os.path.dirname(filepath))
        try: panel['btn_open'].clicked.disconnect() 
        except: pass
        panel['btn_open'].clicked.connect(lambda: os.startfile(filepath) if os.name == 'nt' else os.system(f"open '{filepath}'"))

        res_str = ""
        extra_str = ""
        is_video = ext in ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.m4v', '.webm', '.ts']
        is_image = ext in ['.jpg', '.png', '.jpeg', '.bmp', '.gif', '.webp', '.tiff']

        if is_image:
            try:
                with Image.open(filepath) as im: res_str = f"{im.width}x{im.height}"
                pixmap = QPixmap(filepath)
                if not pixmap.isNull():
                    self.pixmap_cache[cache_key] = pixmap
                    self.update_image_display(panel, pixmap)
                else: panel['img'].setText("Image Error")
            except: panel['img'].setText("Image Error")
        elif is_video:
            try:
                cap = cv2.VideoCapture(filepath)
                if cap.isOpened():
                    w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                    h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                    res_str = f"{w}x{h}"
                    fps = cap.get(cv2.CAP_PROP_FPS)
                    frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
                    if fps > 0 and frame_count > 0:
                        duration_sec = int(frame_count / fps)
                        mins, secs = divmod(duration_sec, 60)
                        extra_str = f"Duration: {mins}:{secs:02d}"
                    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_count // 3) 
                    ret, frame = cap.read()
                    if ret:
                        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        h_img, w_img, ch = frame.shape
                        qimg = QImage(frame.data, w_img, h_img, ch * w_img, QImage.Format.Format_RGB888)
                        pixmap = QPixmap.fromImage(qimg)
                        self.pixmap_cache[cache_key] = pixmap
                        self.update_image_display(panel, pixmap)
                    else: panel['img'].setText("No Preview")
                    cap.release()
                else: panel['img'].setText("Video File")
            except: panel['img'].setText("Video Error")
        else:
            panel['img'].setText(f"📄 {ext} File")
            panel['img'].setStyleSheet("background-color: #222; border: 1px solid #333; color: #555; font-size: 20px;")
        
        details = f"Size: {size_str}"
        if res_str: details += f"  |  Res: {res_str}"
        if extra_str: details += f"  |  {extra_str}"
        panel['details'].setText(details)

    def update_image_display(self, panel, pixmap):
        if pixmap and not pixmap.isNull():
            w = panel['img'].width()
            h = panel['img'].height()
            if w > 10 and h > 10:
                scaled = pixmap.scaled(w, h, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                panel['img'].setPixmap(scaled)

    def delete_file(self, target):
        if self.current_match_index == -1: return
        panel = self.panel_a if target == "A" else self.panel_b
        filepath = panel.get('filepath')
        if not filepath:
            QMessageBox.warning(self, "Error", "No file selected")
            return

        confirm = QMessageBox.question(self, "Confirm Delete", f"Send to Trash?\n\n{filepath}", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if confirm == QMessageBox.StandardButton.Yes:
            try:
                if os.path.exists(filepath):
                    send2trash(filepath)
                self.lbl_status.setText(f"Deleted {os.path.basename(filepath)}")
                self.next_match()
                self.match_list.setFocus()
            except Exception as e:
                QMessageBox.critical(self, "Error", str(e))

    def delete_both_files(self):
        if self.current_match_index == -1: return
        path_a = self.panel_a.get('filepath')
        path_b = self.panel_b.get('filepath')
        if not path_a or not path_b:
            QMessageBox.warning(self, "Error", "Both File A and File B must be available to delete both.")
            return

        confirm = QMessageBox.question(self, "Confirm Delete Both", f"Send both files to Trash?\n\n{path_a}\n{path_b}", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if confirm != QMessageBox.StandardButton.Yes:
            return

        deleted = 0
        for p in (path_a, path_b):
            try:
                if os.path.exists(p):
                    send2trash(p)
                    deleted += 1
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to delete {p}: {e}")

        self.lbl_status.setText(f"Deleted {deleted} file(s)")
        self.next_match()
        self.match_list.setFocus()

    def next_match(self):
        current_row = self.match_list.currentRow()
        if current_row < self.match_list.count() - 1:
            self.match_list.setCurrentRow(current_row + 1)
            self.match_list.scrollToItem(self.match_list.currentItem()) 
        else:
            QMessageBox.information(self, "Done", "No more matches!")

    def get_folder_priority(self, filepath):
        filepath = os.path.normpath(filepath)
        for folder_data in self.scan_folders:
            root = os.path.normpath(folder_data['path'])
            if filepath.startswith(root):
                return folder_data['priority']
        return 0

    def auto_prune_exact(self):
        if self.worker and self.worker.isRunning():
            QMessageBox.warning(self, "Auto-Prune", "Cannot prune while scan is in progress.")
            return

        exact_matches = [m for m in self.matches if m.get('type') == 'EXACT']
        if not exact_matches:
            QMessageBox.information(self, "Auto-Prune", "No exact duplicates found.")
            return

        confirm = QMessageBox.question(
            self,
            "Auto-Prune Exact Duplicates",
            f"This will move {len(exact_matches)} duplicate file(s) to Trash. Continue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        # Delete one duplicate per exact pair (keep highest-priority source)
        files_to_delete = []
        seen = set()
        for match in exact_matches:
            a = match.get('file_a')
            b = match.get('file_b')
            if not a or not b:
                continue

            prio_a = self.get_folder_priority(a)
            prio_b = self.get_folder_priority(b)

            # if equal, keep A and delete B (as generated by the matcher)
            if prio_b > prio_a:
                candidate = a
            else:
                candidate = b

            if candidate and candidate not in seen:
                seen.add(candidate)
                files_to_delete.append(candidate)

        if not files_to_delete:
            QMessageBox.information(self, "Auto-Prune", "No eligible files found for pruning.")
            return

        self.progress_bar.setRange(0, 0)
        self.progress_bar.show()
        self.btn_scan.setEnabled(False)
        self.btn_stop.setEnabled(False)

        self.prune_progress_dialog = QProgressDialog("Pruning exact duplicates...", "Cancel", 0, len(files_to_delete), self)
        self.prune_progress_dialog.setWindowTitle("Auto-Prune Exact Duplicates")
        self.prune_progress_dialog.setWindowModality(Qt.WindowModality.WindowModal)
        self.prune_progress_dialog.setAutoClose(False)
        self.prune_progress_dialog.setAutoReset(False)
        self.prune_progress_dialog.setMinimumDuration(100)
        self.prune_progress_dialog.setValue(0)

        self.prune_worker = AutoPruneWorker(files_to_delete)
        self.prune_worker.progress_update.connect(lambda s: self.lbl_status.setText(s))
        self.prune_worker.progress_value.connect(self.update_progress_bar)
        self.prune_worker.progress_value.connect(lambda current, total: self._update_prune_progress(current, total))
        self.prune_worker.finished.connect(self.on_prune_complete)
        self.prune_worker.error.connect(self.on_error)
        self.prune_worker.aborted.connect(self.on_prune_aborted)

        self.prune_progress_dialog.canceled.connect(lambda: self.prune_worker.stop())
        self.prune_worker.start()

    def _update_prune_progress(self, current, total):
        dialog = self.prune_progress_dialog
        if not dialog:
            return
        try:
            dialog.setMaximum(total)
            dialog.setValue(current)
            dialog.setLabelText(f"Pruning: {current} / {total} files")
        except (RuntimeError, AttributeError):
            # Dialog was closed or garbage collected, ignore stale signal
            pass

    def _close_prune_progress_dialog(self):
        if self.prune_progress_dialog:
            self.prune_progress_dialog.close()
            self.prune_progress_dialog = None
        
        # Disconnect worker signals to prevent stale emissions
        if self.prune_worker:
            try:
                self.prune_worker.progress_value.disconnect()
            except TypeError:
                # Signal already disconnected or not connected
                pass

    def on_prune_complete(self, deleted_count):
        """Called when auto-prune finishes"""
        self._close_prune_progress_dialog()
        self.progress_bar.hide()
        self.btn_scan.setEnabled(True)
        self.btn_stop.setEnabled(False)
        QMessageBox.information(self, "Complete", f"Moved {deleted_count} files to Trash.")
        self.match_list.clear()
        self.matches = []
        self.lbl_status.setText("Pruning complete. Please re-scan.")

    def on_prune_aborted(self):
        """Called when auto-prune is aborted"""
        self._close_prune_progress_dialog()
        self.progress_bar.hide()
        self.btn_scan.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.lbl_status.setText("Pruning aborted.")

    def persist_folder_priorities(self):
        """Save folder priorities to the current database"""
        if self.current_db_path:
            try:
                db = DatabaseManager(self.current_db_path)
                db.save_roots(self.scan_folders)
                db.close()
            except Exception as e:
                print(f"Failed to save priorities: {e}")

    def resizeEvent(self, event):
        if self.current_match_index != -1:
            if self.pixmap_cache['A']:
                self.update_image_display(self.panel_a, self.pixmap_cache['A'])
            if self.pixmap_cache['B']:
                self.update_image_display(self.panel_b, self.pixmap_cache['B'])
        super().resizeEvent(event)

    def closeEvent(self, event):
        # 1) Stop worker and wait (process events while waiting)
        if self.worker and self.worker.isRunning():
            self.worker.stop()
            start = time.time()
            while self.worker.isRunning() and (time.time() - start) < 5.0:
                QApplication.processEvents()
                time.sleep(0.05)

        # Handle prune worker if running
        if hasattr(self, 'prune_worker') and self.prune_worker and self.prune_worker.isRunning():
            self.prune_worker.stop()
            start = time.time()
            while self.prune_worker.isRunning() and (time.time() - start) < 5.0:
                QApplication.processEvents()
                time.sleep(0.05)

        # 1.5) If you hold a DatabaseManager instance, close it here:
        try:
            if hasattr(self, "db") and self.db:
                try:
                    self.db.close()
                except: pass
        except: pass

        # 2) Database cleanup prompt
        if self.current_db_path:
            reply = QMessageBox.question(self, "Cleanup",
                                         "Delete the index database file before exiting?",
                                         QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if reply == QMessageBox.StandardButton.Yes:
                candidates = [
                    self.current_db_path,
                    self.current_db_path + "-shm",
                    self.current_db_path + "-wal"
                ]

                for raw_path in candidates:
                    # Safely strip Windows extended prefix only if present
                    clean_path = raw_path
                    if clean_path.startswith('\\\\?\\'):
                        clean_path = clean_path[4:]

                    # Normalize absolute path
                    clean_path = os.path.abspath(clean_path)

                    if not os.path.exists(clean_path):
                        continue

                    # Try to send to recycle bin, with a small retry loop and user options on failure
                    tried = 0
                    while tried < 3:
                        try:
                            send2trash(clean_path)
                            print(f"Sent to trash: {clean_path}")
                            break
                        except Exception as exc:
                            tried += 1
                            # Ask the user what to do on final failure
                            if tried >= 3:
                                btn = QMessageBox.question(
                                    self, "Failed to Move to Trash",
                                    f"Failed to move to Recycle Bin:\n\n{clean_path}\n\nError: {exc}\n\nChoose:",
                                    QMessageBox.StandardButton.Retry | QMessageBox.StandardButton.No | QMessageBox.StandardButton.Yes
                                )
                                # Map: Retry -> Retry, No -> Skip, Yes -> Delete permanently
                                if btn == QMessageBox.StandardButton.Retry:
                                    tried = 0
                                    continue
                                elif btn == QMessageBox.StandardButton.Yes:
                                    try:
                                        os.remove(clean_path)
                                        print(f"Permanently removed: {clean_path}")
                                    except Exception as e2:
                                        print(f"Permanent delete failed: {e2}")
                                    break
                                else:
                                    # Skip
                                    break
                            else:
                                time.sleep(0.1)

        event.accept()

    def open_github(self):
        """Open the project's GitHub page in the default browser"""
        QDesktopServices.openUrl(QUrl(GITHUB_URL))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = DuplicateFinderApp()
    window.show()
    sys.exit(app.exec())