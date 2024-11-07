import os
import hashlib
import sqlite3
from collections import defaultdict
import concurrent.futures
import threading
from tqdm import tqdm
import platform

# SQLite Database Setup
def setup_database():
    conn = sqlite3.connect('duplicates.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL,
            checksum TEXT NOT NULL,
            size INTEGER NOT NULL,
            file_type TEXT NOT NULL
        )
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_checksum ON files (checksum)
    ''')
    conn.commit()
    return conn

# Function to calculate file checksum (SHA-256)
def calculate_checksum(file_path, chunk_size=4096):
    hash_sha256 = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(chunk_size), b''):
                hash_sha256.update(chunk)
    except Exception as e:
        print(f"Error calculating checksum for {file_path}: {e}")
        return None
    return hash_sha256.hexdigest()

# Function to scan directory and save file info to the database
def scan_directory(directory, conn):
    cursor = conn.cursor()
    file_list = []
    for root, _, files in os.walk(directory):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if os.path.isfile(file_path):
                file_size = os.path.getsize(file_path)
                file_list.append((file_path, file_size))

    # Using thread pool to speed up checksum calculation
    lock = threading.Lock()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for file_path, file_size in file_list:
            futures.append(executor.submit(process_file, file_path, file_size, conn, lock))
        for _ in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Scanning Files"):
            pass
    conn.commit()

# Function to process each file and save info to the database
def process_file(file_path, file_size, conn, lock):
    checksum = calculate_checksum(file_path)
    if checksum:
        file_type = os.path.splitext(file_path)[1].lower()
        with lock:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO files (path, checksum, size, file_type)
                VALUES (?, ?, ?, ?)
            ''', (file_path, checksum, file_size, file_type))

# Function to find duplicates
def find_duplicates(conn):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT checksum, COUNT(*), SUM(size)
        FROM files
        GROUP BY checksum
        HAVING COUNT(*) > 1
    ''')
    duplicates = cursor.fetchall()
    return duplicates

# Function to display summary
def display_summary(conn):
    cursor = conn.cursor()
    duplicates = find_duplicates(conn)
    total_space_recovered = sum(item[2] - item[2] // item[1] for item in duplicates)
    total_duplicates = sum(item[1] - 1 for item in duplicates)
    
    print(f"Total Duplicates Found: {total_duplicates}")
    print(f"Total Space Recovered (bytes): {total_space_recovered}")
    
    cursor.execute('''
        SELECT file_type, COUNT(*)
        FROM files
        GROUP BY file_type
    ''')
    file_type_counts = cursor.fetchall()
    print("\nFile Type Summary:")
    for file_type, count in file_type_counts:
        print(f"{file_type}: {count} files")

# Function to safely delete duplicates using multi-threading for faster deletion
def delete_duplicates(conn):
    cursor = conn.cursor()
    duplicates = find_duplicates(conn)
    files_to_delete = []
    for checksum, _, _ in duplicates:
        cursor.execute('''
            SELECT path FROM files WHERE checksum = ?
        ''', (checksum,))
        paths = cursor.fetchall()
        # Keep one file, delete the rest
        for path in paths[1:]:
            file_path = path[0]
            if input(f"Do you want to delete {file_path}? (y/n): ").lower() == 'y':
                files_to_delete.append(file_path)

    # Using thread pool to delete files concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(delete_file, file_path) for file_path in files_to_delete]
        for _ in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Deleting Files"):
            pass

# Function to delete a file
def delete_file(file_path):
    try:
        os.unlink(file_path)
        print(f"Deleted: {file_path}")
    except Exception as e:
        print(f"Error deleting {file_path}: {e}")

# Function to list available disks (Windows-specific implementation)
def list_disks():
    if platform.system() == "Windows":
        import string
        from ctypes import windll
        drives = []
        bitmask = windll.kernel32.GetLogicalDrives()
        for letter in string.ascii_uppercase:
            if bitmask & 1:
                drives.append(letter + ":\\")
            bitmask >>= 1
        return drives
    else:
        return ["/"]

if __name__ == "__main__":
    print("Available disks:")
    disks = list_disks()
    for idx, disk in enumerate(disks):
        print(f"{idx + 1}: {disk}")
    
    disk_choice = int(input("Select a disk to scan (enter number): "))
    selected_disk = disks[disk_choice - 1]
    
    directory_to_scan = input(f"Enter the folder to scan within {selected_disk} (leave empty to scan entire disk): ")
    if not directory_to_scan:
        directory_to_scan = selected_disk
    else:
        directory_to_scan = os.path.join(selected_disk, directory_to_scan)
    
    conn = setup_database()
    scan_directory(directory_to_scan, conn)
    display_summary(conn)
    if input("Do you want to delete duplicates? (y/n): ").lower() == 'y':
        delete_duplicates(conn)
    conn.close()
