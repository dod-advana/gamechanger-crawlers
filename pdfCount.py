import os

def count_pdfs_in_folder(folder_path):
    # List all files in the given folder
    files = os.listdir(folder_path)
    files_count = len(files)

    # Filter for files that end with .pdf and count them
    pdf_count = sum(1 for file in files if file.lower().endswith('.pdf'))
    return files_count, pdf_count

if __name__ == "__main__":
    folder_path = "./tmp"
    files_count, num_pdfs = count_pdfs_in_folder(folder_path)
    print("There are {} PDF files out of {} in the folder '{}'.".format(num_pdfs, files_count, folder_path))