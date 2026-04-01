# Fuzzy Duplicate Finder
**A (relatively) small Python based program that is capable of scanning for both exact and similiar (fuzzy) duplicate files across multiple folders.** *Including photos, videos, audio and text files.*

## Features include:
1. Simple user interface with buttons to remove one or both (or neither) versions of a found duplicate file.
2. Percentage rating indicating for how similiar the two files are.
3. A function to automatically purge all identical files with weights for the folders (if multiple).
4. Stores a database of previously scanned folders to re-load at another time.
5. Identifies and lists (with option to export) any files that were skipped during a scan.
6. Shows details about the duplicates files including (but not limited to) size, location, and dimensions.

## Running the app
1. Download the [latest release](./releases/latest) for your OS of choice in a compiled format (no dependencies need to be installed) or the loose .py files.
2. For the compiled versions, it is as simple as running the binary. If using the loose files, you will need to refer to the requirements.txt file and install all of them with `python -m pip install -r requirements.txt`. Then simply run `python main.py`.

## A few additional notes:
1. Files are moved to the trash/recycle bin in case of accidental deletion.
2. The program will prompt to optionally remove the database file on close. This too will be sent to the trash or recycle bin.
3. Folder priorites for auto purge are handled by making the highest number the highest priority.
* For example if you have "Folder A" and "Folder B" with priority 10 and 9 respectively, duplicates will be removed from "Folder B". If multiple folders share the same priority, the logic will fall back on the file with the shortest path being the highest priority.
4. When a single folder is selected to scan, the database will be saved inside that folder as "duplicate_index.db". If multiple folders are selected, the program will prompt where you'd like to save it (custom names are supported).

## Screenshots
![Main interface screenshit](/screens/main.png)
![Skipped files popup box screenshot](/screens/skipped.png)

*Photos seen in sample screenshot sourced from [International-dish78](https://www.reddit.com/user/International-dish78/)'s [post](https://www.reddit.com/r/windows/comments/1kmpiox) on /r/windows.*