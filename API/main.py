from fastapi import FastAPI, Query, HTTPException
from typing import Optional, Dict, Any, List
import uvicorn
from MONGO import ProviderDB
from dotenv import load_dotenv
import os

app = FastAPI(
    title="Provider API",
    version="1.0.0"
)

load_dotenv()

mongo_db = ProviderDB(
    connection_string=os.getenv('MONGO_URL'),
    database_name=os.getenv('DATABASE_NAME'),
    collection_name=os.getenv('COLLECTION_NAME')
)

@app.get("/provider/")
async def get_provider(npi: int = Query(..., description="NPI Number")):
    result = await mongo_db.get_by_npi(npi)
    if result is None:
        raise HTTPException(status_code=404, detail="Provider not found")
    if '_id' in result:
        result['_id'] = str(result['_id'])
    return {"provider_data": result}

@app.get("/providers/")
async def get_providers(
    page: int = Query(1, ge=1, description="Номер страницы"),
    page_size: int = Query(100, ge=1, le=1000, description="Количество элементов на странице"),
    sort_field: str = Query("_id", description="Поле для сортировки"),
    sort_direction: int = Query(1, ge=-1, le=1, description="Направление сортировки (1 - по возрастанию, -1 - по убыванию)")
):
    try:
        result = await mongo_db.get_all_providers(
            page=page,
            page_size=page_size,
            sort_field=sort_field,
            sort_direction=sort_direction
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/providers/search/")
async def search_providers(
    search_term: str = Query(..., description="Поисковый запрос"),
    search_fields: Optional[List[str]] = Query(None, description="Поля для поиска"),
    page: int = Query(1, ge=1, description="Номер страницы"),
    page_size: int = Query(100, ge=1, le=1000, description="Количество элементов на странице"),
    sort_field: str = Query("_id", description="Поле для сортировки"),
    sort_direction: int = Query(1, ge=-1, le=1, description="Направление сортировки")
):
    try:
        result = await mongo_db.search_providers(
            search_term=search_term,
            search_fields=search_fields,
            page=page,
            page_size=page_size,
            sort_field=sort_field,
            sort_direction=sort_direction
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/providers/filter/")
async def filter_providers(
    state: Optional[str] = Query(None, description="Штат"),
    city: Optional[str] = Query(None, description="Город"),
    specialty: Optional[str] = Query(None, description="Специальность"),
    organization_name: Optional[str] = Query(None, description="Название организации"),
    page: int = Query(1, ge=1, description="Номер страницы"),
    page_size: int = Query(100, ge=1, le=1000, description="Количество элементов на странице"),
    sort_field: str = Query("_id", description="Поле для сортировки"),
    sort_direction: int = Query(1, ge=-1, le=1, description="Направление сортировки")
):
    try:
        criteria = {}
        
        if state:
            criteria["provider.provider_identification.state"] = state
        if city:
            criteria["provider.provider_identification.city"] = city
        if specialty:
            criteria["provider.specialties.specialty"] = {"$regex": specialty, "$options": "i"}
        if organization_name:
            criteria["provider.provider_identification.organization_name"] = {"$regex": organization_name, "$options": "i"}
        
        result = await mongo_db.get_providers_by_criteria(
            criteria=criteria,
            page=page,
            page_size=page_size,
            sort_field=sort_field,
            sort_direction=sort_direction
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/providers/count/")
async def get_providers_count(
    state: Optional[str] = Query(None, description="Штат для фильтрации"),
    city: Optional[str] = Query(None, description="Город для фильтрации"),
    specialty: Optional[str] = Query(None, description="Специальность для фильтрации")
):
    try:
        criteria = {}
        
        if state:
            criteria["provider.provider_identification.state"] = state
        if city:
            criteria["provider.provider_identification.city"] = city
        if specialty:
            criteria["provider.specialties.specialty"] = {"$regex": specialty, "$options": "i"}
        
        count = await mongo_db.get_providers_count(criteria if criteria else None)
        return {"total_count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/providers/by-state/{state}")
async def get_providers_by_state(
    state: str,
    page: int = Query(1, ge=1, description="Номер страницы"),
    page_size: int = Query(100, ge=1, le=1000, description="Количество элементов на странице")
):
    try:
        criteria = {"provider.provider_identification.state": state}
        result = await mongo_db.get_providers_by_criteria(
            criteria=criteria,
            page=page,
            page_size=page_size
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/providers/by-specialty/{specialty}")
async def get_providers_by_specialty(
    specialty: str,
    page: int = Query(1, ge=1, description="Номер страницы"),
    page_size: int = Query(100, ge=1, le=1000, description="Количество элементов на странице")
):
    try:
        criteria = {"provider.specialties.specialty": {"$regex": specialty, "$options": "i"}}
        result = await mongo_db.get_providers_by_criteria(
            criteria=criteria,
            page=page,
            page_size=page_size
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/providers/minimal/")
async def get_providers_minimal(
    page: int = Query(1, ge=1, description="Номер страницы"),
    page_size: int = Query(100, ge=1, le=1000, description="Количество элементов на странице")
):
    try:
        projection = {
            "provider.provider_identification.npi": 1,
            "provider.provider_identification.organization_name": 1,
            "provider.provider_identification.individual_name": 1,
            "provider.provider_identification.state": 1,
            "provider.provider_identification.city": 1,
            "provider.specialties.specialty": 1
        }
        
        result = await mongo_db.get_all_providers(
            page=page,
            page_size=page_size,
            projection=projection
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health/")
async def health_check():
    try:
        count = await mongo_db.get_providers_count()
        return {
            "status": "healthy",
            "database": "connected",
            "total_providers": count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.on_event("shutdown")
async def shutdown_event():
    await mongo_db.close()

def run():
    host = os.getenv('HOST') or "127.0.0.1"
    port_str = os.getenv('PORT')
    port = int(port_str) if port_str and port_str.isdigit() else 3000
    uvicorn.run("main:app", host=host, port=port, reload=True)

if __name__ == "__main__":
    run()