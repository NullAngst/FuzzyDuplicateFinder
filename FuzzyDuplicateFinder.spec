# -*- mode: python ; coding: utf-8 -*-
import sys
from PyInstaller.building.build_main import Analysis
from PyInstaller.building.api import EXE, PYZ

# Platform detection
is_macos = sys.platform == 'darwin'
is_windows = sys.platform == 'win32'
is_linux = sys.platform.startswith('linux')

# Only import BUNDLE on macOS
if is_macos:
    from PyInstaller.building.osx import BUNDLE

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[
        'librosa',
        'librosa.core',
        'librosa.feature',
        'numpy',
        'cv2',
        'imagehash',
        'PIL',
        'PyQt6',
        'PyQt6.QtCore',
        'PyQt6.QtGui',
        'PyQt6.QtWidgets',
        'send2trash',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludedimports=[],
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=None)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='FuzzyDuplicateFinder',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=is_windows,  # Only use UPX on Windows
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None if not is_macos else 'Apple Development',
    entitlements_file=None,
)

# Only create BUNDLE on macOS
if is_macos:
    app = BUNDLE(
        exe,
        name='FuzzyDuplicateFinder.app',
        icon=None,
        bundle_identifier='com.fuzzyduplicate.finder',
        info_plist={
            'NSPrincipalClass': 'NSApplication',
            'NSHighResolutionCapable': 'True',
        },
        codesign_identity=None,
    )