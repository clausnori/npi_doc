import requests
import re
import os
from dotenv import load_dotenv
from NPI import NPI_Load, Verified, Mapper
from MONGO import ProviderDB
import asyncio

load_dotenv()

#ENV VARIABLES
MONGO_URL = os.getenv("MONGO_URL")
DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

#NPPES ZIP CONFIG
NPPES_LISTING_URL = "https://download.cms.gov/nppes/NPI_Files.html"
NPPES_BASE_URL = "https://download.cms.gov/nppes/"
NPPES_PATTERN = re.compile(r'NPPES_Data_Dissemination_(\d+_\d+)_Weekly\.zip')

#CMS CSV CONFIG
CMS_DATASET_ID = "mj5m-pzi6"
CMS_META_URL = f"https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/{CMS_DATASET_ID}"
CMS_CSV_URL = f"https://data.cms.gov/provider-data/api/1/datastore_export/csv?dataset={CMS_DATASET_ID}"
CMS_META_FILE = f"{CMS_DATASET_ID}.meta"
CMS_CSV_FILE = f"{CMS_DATASET_ID}.csv"

def download_file(url, filename):
    print(f"Downloading: {filename}")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(filename, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded: {filename}")

#NPPES ZIP
def get_local_zip_suffix():
    for fname in os.listdir('.'):
        match = NPPES_PATTERN.fullmatch(fname)
        if match:
            return match.group(1)
    return None

def get_latest_remote_zip_name():
    response = requests.get(NPPES_LISTING_URL)
    response.raise_for_status()
    matches = NPPES_PATTERN.findall(response.text)
    if not matches:
        print("No ZIP found on NPPES page.")
        return None
    matches.sort(reverse=True)
    return f"NPPES_Data_Dissemination_{matches[0]}_Weekly.zip"

def update_database(zip_filename):
    print(f"Updating DB with: {zip_filename}")
    load = NPI_Load(zip_filename, "npidata")
    mapper = Mapper()
    tools = Verified()
    mongo_db = ProviderDB(
        connection_string=MONGO_URL,
        database_name=DATABASE_NAME,
        collection_name=COLLECTION_NAME
    )
    schema = load.get_schema_from_sample()
    for_type = tools.type_code(schema)

    for chunk in load.read_csv_in_chunks(chunk_size=100):
        for idx in range(len(chunk)):
            row_df = chunk.iloc[[idx]]
            cms_data = mapper.map(row_df, for_type)
            print(f"Updating record #{idx}")
            mongo_db.merge_or_insert_one(cms_data[0])
    print("ZIP-based DB update complete.")


def get_cms_last_modified():
    try:
        resp = requests.get(CMS_META_URL)
        resp.raise_for_status()
        data = resp.json()
        return data.get("mtch_modified") or data.get("modified")
    except Exception as e:
        print(f"Metadata fetch failed: {e}")
        return None

def update_database_from_csv():
    print(f"Updating DB from CSV: {CMS_CSV_FILE}")
    load = NPI_Load(CMS_CSV_FILE, "npidata")
    mapper = Mapper()
    tools = Verified()
    mongo_db = ProviderDB(
        connection_string=MONGO_URL,
        database_name=DATABASE_NAME,
        collection_name=COLLECTION_NAME
    )
    schema = load.get_schema_from_sample()
    for_type = tools.type_code(schema)

    for chunk in load.read_csv_in_chunks(chunk_size=100):
        for idx in range(len(chunk)):
            row_df = chunk.iloc[[idx]]
            cms_data = mapper.map(row_df, for_type)
            print(f"Updating record #{idx}")
            mongo_db.merge_or_insert_one(cms_data[0])
    print("CSV-based DB update complete.")


def main():
    local_suffix = get_local_zip_suffix()
    latest_zip = get_latest_remote_zip_name()

    if latest_zip:
        if os.path.exists(latest_zip):
            print(f"Local ZIP exists: {latest_zip}")
            update_database(latest_zip)
        elif local_suffix and f"NPPES_Data_Dissemination_{local_suffix}_Weekly.zip" == latest_zip:
            print("Local ZIP is up to date.")
            update_database(f"NPPES_Data_Dissemination_{local_suffix}_Weekly.zip")
        else:
            download_file(NPPES_BASE_URL + latest_zip, latest_zip)
            update_database(latest_zip)
 
 
    remote_modified = get_cms_last_modified()
    local_modified = None

    if os.path.exists(CMS_META_FILE):
        with open(CMS_META_FILE, "r") as f:
            local_modified = f.read().strip()

    if remote_modified and remote_modified != local_modified:
        print(f"New CMS dataset version: {remote_modified}")
        download_file(CMS_CSV_URL, CMS_CSV_FILE)
        update_database_from_csv()
        with open(CMS_META_FILE, "w") as f:
            f.write(remote_modified)
    else:
        print("CMS CSV is up to date.")

if __name__ == "__main__":
    asyncio.run(main())