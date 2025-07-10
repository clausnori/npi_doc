from NPI import NPI_Load, Verified, Mapper
from MONGO import ProviderDB

load = NPI_Load("NPPES_Data_Dissemination_June_2025.zip", "npidata")

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
        mongo_db.merge_or_insert_one(cms_data[0])