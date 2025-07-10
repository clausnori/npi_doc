from fastapi import FastAPI, Query
from typing import Optional
import uvicorn
from MONGO import ProviderDB
from dotenv import load_dotenv
import os


app = FastAPI()
load_dotenv()

mongo_db = ProviderDB(
    connection_string=os.getenv('MONGO_URL'),
    database_name=os.getenv('DATABASE_NAME'),
    collection_name=os.getenv('COLLECTION_NAME')
)

@app.get("/provider/")
async def get_provider(npi: int = Query(...)):
    result = await mongo_db.get_by_npi(npi)
    if result is None:
        return {"error": "Provider not found"}
    if '_id' in result:
        result['_id'] = str(result['_id'])
    return {"provider_data": result}


def run():
    host = os.getenv('HOST') or "127.0.0.1"
    port_str = os.getenv('PORT')
    port = int(port_str) if port_str and port_str.isdigit() else 3000
    uvicorn.run("main:app", host=host, port=port, reload=True)

if __name__ == "__main__":
    run()