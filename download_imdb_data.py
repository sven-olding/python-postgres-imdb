import os
import requests
from tqdm import tqdm

BASE_URL = "https://datasets.imdbws.com/"

FILES = [
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz"
]

DOWNLOAD_DIR = "imdb_data"

def download_file(url, dest_path):
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    with open(dest_path, 'wb') as file, tqdm(
        desc=f"Downloading {os.path.basename(dest_path)}",
        total=total_size,
        unit='B',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in response.iter_content(chunk_size=1024):
            file.write(data)
            bar.update(len(data))

def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    for file_name in FILES:
        file_url = BASE_URL + file_name
        dest_path = os.path.join(DOWNLOAD_DIR, file_name)
        print(f"Downloading {file_name}...")
        download_file(file_url, dest_path)
        print(f"Downloaded {file_name} to {dest_path}")

if __name__ == "__main__":
    main()