from NPI import NPI_Load , Verified, Mapper 
from MONGO import ProviderDB
from collections import defaultdict





load = NPI_Load("NPPES_Data_Dissemination_June_2025.zip","npidata")

#For PECOS DATA
#load = NPI_Load("DAC_NationalDownloadableFile.csv")
maper = Mapper()
tools = Verified()

mongo_db = ProviderDB(
          connection_string="",
          database_name="",
          collection_name="")
          
#collections for test = "ppp"
shema = load.get_schema_from_sample()
for_type = tools.type_code(shema)

for i, chunk in enumerate(load.read_csv_in_chunks(chunk_size=100)):
        for idx in range(len(chunk)):
            row_df = chunk.iloc[[idx]]
            cms_data = maper.map(row_df, for_type)
            mongo_db.merge_or_insert_one(cms_data[0])
        break

"""
tools = Verified()


header = load.read_csv_head()
shema = load.get_schema_from_sample()
print(header)
print(shema)
print("#"*10)
t = tools.type_code(shema)
"""

