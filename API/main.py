#TODO
#Api from PECOS 
#Load zip cron from NPI


from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class Provider(BaseModel):
    npi: str
    active: bool = True

@app.post("/provider/")
async def create_provider(provider: Provider):
    return {"received": provider}
    
    
  