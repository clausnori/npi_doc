from pymongo import MongoClient
from typing import List, Dict, Optional, Any
from bson import ObjectId
import datetime
import json
import hashlib

class ProviderDB:
    def __init__(self, connection_string: str, database_name: str, collection_name: str = "providers"):
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]
        self.collection.create_index("provider.provider_identification.npi", unique=True)
    
    def _generate_data_hash(self, data: Dict[str, Any]) -> str:
        provider = data.get("provider", data)
        meta = provider.get("meta_info", {}).copy()
        meta.pop("last_update", None)

        provider_copy = provider.copy()
        provider_copy["meta_info"] = meta

        json_str = json.dumps(provider_copy, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()
        
    def _merge_providers(self, old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        def is_empty(value):
            """For  empty value"""
            return value in [None, "", [], {}]
        
        def normalize_value(value):
            """norm data for comparison"""
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
                            merged[key] = merged_nestedы
                    elif isinstance(new_value, list) and isinstance(old_value, list):
                        if new_value:
                            combined = old_value + new_value
                            merged[key] = list(dict.fromkeys(combined))
                                       
                    # For all other cases - compare normalized values
                    else:
                        if key == "last_update":
                            # For last_time we pick later time
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
    
        return deep_merge(old, new)
    
    def get_by_npi(self, npi) -> Optional[Dict[str, Any]]:
        if isinstance(npi, str) and npi.isdigit():
            result = self.collection.find_one({"provider.provider_identification.npi": int(npi)})
            if result:
                return result
        elif isinstance(npi, int):
            result = self.collection.find_one({"provider.provider_identification.npi": str(npi)})
            if result:
                return result
        
        return self.collection.find_one({"provider.provider_identification.npi": npi})
        
    def merge_or_insert_many(self, providers: List[Dict[str, Any]]) -> List[str]:
        """
        Merge many data after mapping 
        """
        updated_ids = []
    
        for provider_data in providers:
            npi = provider_data.get("provider_identification", {}).get("npi")
            if not npi:
                continue
    
            existing = self.get_by_npi(npi)
    
            provider_data.setdefault("meta_info", {})
            provider_data["meta_info"]["last_update"] = datetime.datetime.utcnow().isoformat()
            provider_data["meta_info"]["data_hash"] = self._generate_data_hash(provider_data)
    
            if not existing:
                result = self.collection.insert_one({"provider": provider_data})
                updated_ids.append(str(result.inserted_id))
                continue
    
            existing_provider = existing.get("provider", {})
            merged = self._merge_providers(existing_provider, provider_data)
            
            if merged != existing_provider:
                self.collection.update_one(
                    {"provider.provider_identification.npi": npi},
                    {"$set": {"provider": merged}}
                )
                updated_ids.append(str(existing["_id"]))
    
        return updated_ids
    
    def merge_or_insert_one(self, provider_data: Dict[str, Any]) -> Optional[str]:
        """
        Merge or update Data for once data after mapping 
        """
        npi = provider_data.get("provider_identification", {}).get("npi")
        if not npi:
            return None
    
        provider_data.setdefault("meta_info", {})
        provider_data["meta_info"]["last_update"] = datetime.datetime.utcnow().isoformat()
        provider_data["meta_info"]["data_hash"] = self._generate_data_hash(provider_data)
    
        existing = self.get_by_npi(npi)
        if not existing:
            result = self.collection.insert_one({"provider": provider_data})
            print(f"Inserted new provider with NPI: {npi}")
            return str(result.inserted_id)
    
        existing_provider = existing.get("provider", {})
        merged = self._merge_providers(existing_provider, provider_data)
        if merged != existing_provider:
            existing_npi = existing_provider.get("provider_identification", {}).get("npi")
            if existing_npi and isinstance(existing_npi, int):
                merged_npi = merged.get("provider_identification", {}).get("npi")
                if isinstance(merged_npi, str) and merged_npi.isdigit():
                    merged["provider_identification"]["npi"] = int(merged_npi)
            
            self.collection.update_one(
                {"provider.provider_identification.npi": existing_npi},
                {"$set": {"provider": merged}}
            )
            print(f"Updated provider with NPI: {npi}")
            return str(existing["_id"])
        
        print(f"No changes for provider with NPI: {npi}")
        return str(existing["_id"])
    
    def close(self):
        self.client.close()