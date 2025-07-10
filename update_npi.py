import requests
import re
import os
from NPI import NPI_Load, Verified, Mapper
from MONGO import ProviderDB
import asyncio
from dotenv import load_dotenv
import os

LISTING_URL = "https://download.cms.gov/nppes/NPI_Files.html"
BASE_URL = "https://download.cms.gov/nppes/"
PATTERN = re.compile(r'NPPES_Data_Dissemination_(\d+_\d+)_Weekly\.zip')

def get_local_zip_suffix():
    for fname in os.listdir('.'):
        match = PATTERN.fullmatch(fname)
        if match:
            return match.group(1)
    return None

def get_latest_remote_zip_name():
    response = requests.get(LISTING_URL)
    response.raise_for_status()

    matches = PATTERN.findall(response.text)
    if not matches:
        print("NONE FIND ZIP IN PAGE")
        return None

    matches.sort(reverse=True)
    return f"NPPES_Data_Dissemination_{matches[0]}_Weekly.zip"

def download_file(url, filename):
    print(f"LOAD : {filename}")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(filename, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"DOWNLOAD : {filename}")

def update_database(zip_filename):
    print(f"UPDATE DATA IN DB {zip_filename}...")
    load = NPI_Load(zip_filename, "npidata")
    mapper = Mapper()
    tools = Verified()
    mongo_db = ProviderDB(
        connection_string="",
        database_name="",
        collection_name=""
    )
    schema = load.get_schema_from_sample()
    for_type = tools.type_code(schema)

    for chunk in load.read_csv_in_chunks(chunk_size=100):
        for idx in range(len(chunk)):
            row_df = chunk.iloc[[idx]]
            cms_data = mapper.map(row_df, for_type)
            print(f"UPDATE RECORD {idx}")
            mongo_db.merge_or_insert_one(cms_data[0])
    print("UPDATE DB :OK ")

def main():
    local_suffix = get_local_zip_suffix()
    latest_filename = get_latest_remote_zip_name()

    if not latest_filename:
        return

    if os.path.exists(latest_filename):
        print(f"FILE WITH CURRENT DATA  exists : {latest_filename}")
        update_dDATA FILE is the latest 
        return

    if local_suffix and f"NPPES_Data_Dissemination_{local_suffix}_Weekly.zip" == latest_filename:
        print("DATA FILE is the latest ")
        update_database(f"NPPES_Data_Dissemination_{local_suffix}_Weekly.zip")
        return

    download_file(BASE_URL + latest_filename, latest_filename)
    update_database(latest_filename)

if __name__ == "__main__":
    main()