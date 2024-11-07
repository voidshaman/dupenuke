import os
import shutil
import sys

def duplicate_folder(source_folder, number_of_copies, destination_folder):
    if not os.path.exists(source_folder):
        print(f"Source folder '{source_folder}' does not exist.")
        sys.exit(1)
    if not os.path.isdir(source_folder):
        print(f"'{source_folder}' is not a folder.")
        sys.exit(1)

    os.makedirs(destination_folder, exist_ok=True)

    for i in range(1, number_of_copies + 1):
        dest_path = os.path.join(destination_folder, f"copy_{i}")
        shutil.copytree(source_folder, dest_path)
        print(f"Created: {dest_path}")

    print(f"\nSuccessfully created {number_of_copies} copies in '{destination_folder}'.")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python duplicate_folders.py <source_folder> <number_of_copies> <destination_folder>")
        sys.exit(1)

    source = sys.argv[1]
    copies = int(sys.argv[2])
    destination = sys.argv[3]

    duplicate_folder(source, copies, destination)