import aiohttp
import aiofiles
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

async def download_file(session, url, filename):
    print(f"Downloading: {filename}")
    async with session.get(url) as response:
        response.raise_for_status()
        async with aiofiles.open(filename, "wb") as f:
            async for chunk in response.content.iter_chunked(8192):
                await f.write(chunk)
    print(f"Downloaded: {filename}")

#NPPES ZIP
def get_local_zip_suffix():
    for fname in os.listdir('.'):
        match = NPPES_PATTERN.fullmatch(fname)
        if match:
            return match.group(1)
    return None

async def get_latest_remote_zip_name(session):
    async with session.get(NPPES_LISTING_URL) as response:
        response.raise_for_status()
        text = await response.text()
        matches = NPPES_PATTERN.findall(text)
        if not matches:
            print("No ZIP found on NPPES page.")
            return None
        matches.sort(reverse=True)
        return f"NPPES_Data_Dissemination_{matches[0]}_Weekly.zip"

async def update_database(zip_filename):
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
            await mongo_db.merge_or_insert_one(cms_data[0])
    print("ZIP-based DB update complete.")

async def get_cms_last_modified(session):
    try:
        async with session.get(CMS_META_URL) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get("mtch_modified") or data.get("modified")
    except Exception as e:
        print(f"Metadata fetch failed: {e}")
        return None

async def update_database_from_csv():
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
            await mongo_db.merge_or_insert_one(cms_data[0])
    print("CSV-based DB update complete.")

async def read_file_async(filename):
    """Helper function to read file asynchronously"""
    try:
        async with aiofiles.open(filename, "r") as f:
            return await f.read()
    except FileNotFoundError:
        return None

async def write_file_async(filename, content):
    """Helper function to write file asynchronously"""
    async with aiofiles.open(filename, "w") as f:
        await f.write(content)

async def main():
    async with aiohttp.ClientSession() as session:
        # Handle NPPES ZIP updates
        local_suffix = get_local_zip_suffix()
        latest_zip = await get_latest_remote_zip_name(session)

        if latest_zip:
            if os.path.exists(latest_zip):
                print(f"Local ZIP exists: {latest_zip}")
                await update_database(latest_zip)
            elif local_suffix and f"NPPES_Data_Dissemination_{local_suffix}_Weekly.zip" == latest_zip:
                print("Local ZIP is up to date.")
                await update_database(f"NPPES_Data_Dissemination_{local_suffix}_Weekly.zip")
            else:
                await download_file(session, NPPES_BASE_URL + latest_zip, latest_zip)
                await update_database(latest_zip)
        
        # Handle CMS CSV updates
        remote_modified = await get_cms_last_modified(session)
        local_modified = await read_file_async(CMS_META_FILE)
        
        if local_modified:
            local_modified = local_modified.strip()

        if remote_modified and remote_modified != local_modified:
            print(f"New CMS dataset version: {remote_modified}")
            await download_file(session, CMS_CSV_URL, CMS_CSV_FILE)
            await update_database_from_csv()
            await write_file_async(CMS_META_FILE, remote_modified)
        else:
            print("CMS CSV is up to date.")

if __name__ == "__main__":
    asyncio.run(main())