from motor.motor_asyncio import AsyncIOMotorClient
from typing import List, Dict, Optional, Any
from bson import ObjectId
import datetime
import json
import hashlib

class ProviderDB:
    def __init__(self, connection_string: str, database_name: str, collection_name: str = "providers"):
        self.client = AsyncIOMotorClient(connection_string)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]
        self._index_created = False
    
    async def _ensure_index(self):
        if not self._index_created:
            await self.collection.create_index("provider_identification.npi", unique=True)
            self._index_created = True
    
    def _generate_data_hash(self, data: Dict[str, Any]) -> str:
        meta = data.get("meta_info", {}).copy()
        meta.pop("last_update", None)

        data_copy = data.copy()
        data_copy["meta_info"] = meta

        json_str = json.dumps(data_copy, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()
    
    def _normalize_address_structure(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(data, dict):
            return data
        
        normalized = data.copy()
        
        if "business_addresses" in normalized:
            business_addresses = normalized["business_addresses"]
            
            for addr_type in ["mailing_address", "practice_location"]:
                if addr_type in business_addresses:
                    addr_data = business_addresses[addr_type]
                    
                    if isinstance(addr_data, dict) and addr_type in addr_data:
                        business_addresses[addr_type] = addr_data[addr_type]
        
        return normalized
        
    def _merge_providers(self, old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        def is_empty(value):
            return value in [None, "", [], {}]
        
        def normalize_value(value):
            if isinstance(value, str) and value.isdigit():
                return int(value)
            return value
    
        def deep_merge(old_data, new_data):
            if isinstance(old_data, dict) and isinstance(new_data, dict):
                merged = old_data.copy()
                
                for key, new_value in new_data.items():
                    old_value = merged.get(key)
                    
                    if key not in merged or is_empty(old_value):
                        if not is_empty(new_value):
                            merged[key] = new_value
                    elif is_empty(new_value):
                        continue
                    elif isinstance(new_value, dict) and isinstance(old_value, dict):
                        merged_nested = deep_merge(old_value, new_value)
                        if merged_nested != old_value:
                            merged[key] = merged_nested
                    elif isinstance(new_value, list) and isinstance(old_value, list):
                        if new_value:
                            combined = old_value + new_value
                            merged[key] = list(dict.fromkeys(combined))
                                       
                    else:
                        if key == "last_update":
                            try:
                                old_time = datetime.datetime.fromisoformat(str(old_value))
                                new_time = datetime.datetime.fromisoformat(str(new_value))
                                if new_time > old_time:
                                    merged[key] = new_value
                            except:
                                merged[key] = new_value
                        else:
                            normalized_old = normalize_value(old_value)
                            normalized_new = normalize_value(new_value)
                            
                            if normalized_new != normalized_old:
                                merged[key] = new_value
                
                return merged
            else:
                return new_data if not is_empty(new_data) else old_data
        
        normalized_old = self._normalize_address_structure(old)
        normalized_new = self._normalize_address_structure(new)
        
        merged = deep_merge(normalized_old, normalized_new)
        
        return self._normalize_address_structure(merged)
    
    def _remove_nested_ids(self, obj):
        """Remove _id fields from nested objects while preserving the main document _id"""
        if isinstance(obj, dict):
            keys_to_remove = []
            for key, value in obj.items():
                if key == "_id" and isinstance(value, ObjectId):
                    # Skip - this is likely a nested object's _id
                    keys_to_remove.append(key)
                elif isinstance(value, (dict, list)):
                    self._remove_nested_ids(value)
            
            # Remove nested _id fields
            for key in keys_to_remove:
                if key != "_id" or not isinstance(obj.get("_id"), str):
                    # Only remove if it's not the main document _id (which should be string)
                    obj.pop(key, None)
                    
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    # Remove _id from objects in arrays
                    if "_id" in item:
                        item.pop("_id", None)
                    self._remove_nested_ids(item)
                elif isinstance(item, list):
                    self._remove_nested_ids(item)

    async def get_by_npi(self, npi) -> Optional[Dict[str, Any]]:
        await self._ensure_index()
        if isinstance(npi, str) and npi.isdigit():
            result = await self.collection.find_one({"provider_identification.npi": int(npi)})
            if result:
                result["_id"] = str(result["_id"])
                self._remove_nested_ids(result)
                return result
        elif isinstance(npi, int):
            result = await self.collection.find_one({"provider_identification.npi": str(npi)})
            if result:
                result["_id"] = str(result["_id"])
                self._remove_nested_ids(result)
                return result
        
        result = await self.collection.find_one({"provider_identification.npi": npi})
        if result:
            result["_id"] = str(result["_id"])
            self._remove_nested_ids(result)
        return result
        
    async def merge_or_insert_many(self, providers: List[Dict[str, Any]]) -> List[str]:
        await self._ensure_index()
        updated_ids = []
    
        for provider_data in providers:
            npi = provider_data.get("provider_identification", {}).get("npi")
            if not npi:
                continue
                
            provider_data = self._normalize_address_structure(provider_data)
    
            existing = await self.get_by_npi(npi)
    
            provider_data.setdefault("meta_info", {})
            provider_data["meta_info"]["last_update"] = datetime.datetime.utcnow().isoformat()
            provider_data["meta_info"]["data_hash"] = self._generate_data_hash(provider_data)
    
            if not existing:
                result = await self.collection.insert_one(provider_data)
                updated_ids.append(str(result.inserted_id))
                continue
    
            merged = self._merge_providers(existing, provider_data)
            
            if merged != existing:
                await self.collection.update_one(
                    {"provider_identification.npi": npi},
                    {"$set": merged}
                )
                updated_ids.append(str(existing["_id"]))
    
        return updated_ids
    
    async def merge_or_insert_one(self, provider_data: Dict[str, Any]) -> Optional[str]:
        await self._ensure_index()
        
        npi = provider_data.get("provider_identification", {}).get("npi")
        if not npi:
            return None

        provider_data = self._normalize_address_structure(provider_data)

        provider_data.setdefault("meta_info", {})
        provider_data["meta_info"]["last_update"] = datetime.datetime.utcnow().isoformat()
        provider_data["meta_info"]["data_hash"] = self._generate_data_hash(provider_data)

        existing = await self.get_by_npi(npi)
        if not existing:
            result = await self.collection.insert_one(provider_data)
            print(f"Inserted new provider with NPI: {npi}")
            return str(result.inserted_id)

        merged = self._merge_providers(existing, provider_data)
        if merged != existing:
            existing_npi = existing.get("provider_identification", {}).get("npi")
            if existing_npi and isinstance(existing_npi, int):
                merged_npi = merged.get("provider_identification", {}).get("npi")
                if isinstance(merged_npi, str) and merged_npi.isdigit():
                    merged["provider_identification"]["npi"] = int(merged_npi)
            
            # Remove _id from merged data before updating
            update_data = merged.copy()
            update_data.pop("_id", None)
            
            await self.collection.update_one(
                {"provider_identification.npi": existing_npi},
                {"$set": update_data}
            )
            print(f"Updated provider with NPI: {npi}")
            return str(existing["_id"])
        
        print(f"No changes for provider with NPI: {npi}")
        return str(existing["_id"])

    async def get_all_providers(
        self, 
        page: int = 1, 
        page_size: int = 100, 
        filter_query: Optional[Dict[str, Any]] = None,
        sort_field: str = "_id",
        sort_direction: int = 1,
        projection: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        await self._ensure_index()
        
        if page < 1:
            page = 1
        if page_size < 1:
            page_size = 100
        if page_size > 1000:
            page_size = 1000
        
        query = filter_query or {}
        total_items = await self.collection.count_documents(query)
        total_pages = (total_items + page_size - 1) // page_size
        skip = (page - 1) * page_size
        
        cursor = self.collection.find(
            query,
            projection
        ).sort(sort_field, sort_direction).skip(skip).limit(page_size)
        
        data = await cursor.to_list(length=page_size)
        
        for item in data:
            if "_id" in item:
                item["_id"] = str(item["_id"])
            # Recursively remove _id from nested objects
            self._remove_nested_ids(item)
        
        return {
            "data": data,
            "pagination": {
                "current_page": page,
                "page_size": page_size,
                "total_items": total_items,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
        }

    async def get_providers_by_criteria(
        self,
        criteria: Dict[str, Any],
        page: int = 1,
        page_size: int = 100,
        sort_field: str = "_id",
        sort_direction: int = 1
    ) -> Dict[str, Any]:
        return await self.get_all_providers(
            page=page,
            page_size=page_size,
            filter_query=criteria,
            sort_field=sort_field,
            sort_direction=sort_direction
        )

    async def search_providers(
        self,
        search_term: str,
        search_fields: List[str] = None,
        page: int = 1,
        page_size: int = 100,
        sort_field: str = "_id",
        sort_direction: int = 1
    ) -> Dict[str, Any]:
        if not search_fields:
            search_fields = [
                "provider_identification.organization_name",
                "provider_identification.individual_name.first_name",
                "provider_identification.individual_name.last_name",
                "provider_identification.individual_name.middle_name"
            ]
        
        search_query = {
            "$or": [
                {field: {"$regex": search_term, "$options": "i"}} 
                for field in search_fields
            ]
        }
        
        return await self.get_all_providers(
            page=page,
            page_size=page_size,
            filter_query=search_query,
            sort_field=sort_field,
            sort_direction=sort_direction
        )

    async def get_providers_count(self, filter_query: Optional[Dict[str, Any]] = None) -> int:
        await self._ensure_index()
        query = filter_query or {}
        return await self.collection.count_documents(query)
    
    async def close(self):
        self.client.close()