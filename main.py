from NPI import NPI_Load, Verified, Mapper
from MONGO import ProviderDB
import asyncio
from dotenv import load_dotenv
import os

load_dotenv()

async def main():
    load = NPI_Load("NPPES_Data_Dissemination_063025_070625_Weekly.zip", "npidata")
    mapper = Mapper()
    tools = Verified()
    mongo_db = ProviderDB(
      connection_string=os.getenv('MONGO_URL'),
      database_name=os.getenv('DATABASE_NAME'),
      collection_name=os.getenv('COLLECTION_NAME')
    )
    schema = load.get_schema_from_sample()
    for_type = tools.type_code(schema)
    for chunk in load.read_csv_in_chunks(chunk_size=100):
      for idx in range(len(chunk)):
          row_df = chunk.iloc[[idx]]
          cms_data = mapper.map(row_df, for_type)
          print(cms_data)
          results = await mongo_db.merge_or_insert_one(cms_data[0])
    await db.close()
    
if __name__ == "__main__":
  asyncio.run(main())
