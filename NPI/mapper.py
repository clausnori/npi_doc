import pandas as pd
from pandas import DataFrame
import json
from datetime import datetime
import json
import hashlib

# Create mapper instance
#mapper = Mapper()
# For CMS DataFrame
# cms_data = mapper.map(cms_df, "CMS")
# For NPI DataFrame  
# npi_data = mapper.map(npi_df, "NPI")
# Convert to JSON
# json_output = json.dumps(cms_data, indent=2, ensure_ascii=False)

class Mapper:
    def __init__(self):
        # Mapping for CMS DataFrame columns
        self.cms_mapping = {
            'npi':'NPI',
            'pac_id': 'Ind_PAC_ID',
            'enrollment_id': 'Ind_enrl_ID',
            'last_name': 'Provider Last Name',
            'first_name': 'Provider First Name',
            'middle_name': 'Provider Middle Name',
            'suffix': 'suff',
            'gender': 'gndr',
            'credentials': 'Cred',
            'medical_school': 'Med_sch',
            'graduation_year': 'Grd_yr',
            'primary_specialty': 'pri_spec',
            'secondary_specialty_1': 'sec_spec_1',
            'secondary_specialty_2': 'sec_spec_2',
            'secondary_specialty_3': 'sec_spec_3',
            'secondary_specialty_4': 'sec_spec_4',
            'secondary_specialty_all': 'sec_spec_all',
            'facility_name': 'Facility Name',
            'facility_pac_id': 'org_pac_id',
            'organization_members_count': 'num_org_mem',
            'practice_address_line_1': 'adr_ln_1',
            'practice_address_line_2': 'adr_ln_2',
            'practice_address_city': 'City/Town',
            'practice_address_state': 'State',
            'practice_address_zip_code': 'ZIP Code',
            'practice_address_phone': 'Telephone Number',
            'practice_address_id': 'adrs_id',
            'individual_assignment': 'ind_assgn',
            'group_assignment': 'grp_assgn',
            'telehealth_eligible': 'Telehlth'
        }
        
        # Mapping for NPI DataFrame columns
        self.npi_mapping = {
            'npi': 'NPI',
            'entity_type_code': 'Entity Type Code',
            'last_name': 'Provider Last Name (Legal Name)',
            'first_name': 'Provider First Name',
            'middle_name': 'Provider Middle Name',
            'suffix': 'Provider Name Suffix Text',
            'gender': 'Provider Sex Code',
            'credentials': 'Provider Credential Text',
            'taxonomy_code': 'Healthcare Provider Taxonomy Code_1',
            'taxonomy_primary': 'Healthcare Provider Primary Taxonomy Switch_1',
            'license_number': 'Provider License Number_1',
            'license_state': 'Provider License Number State Code_1',
            'mailing_line_1': 'Provider First Line Business Mailing Address',
            'mailing_line_2': 'Provider Second Line Business Mailing Address',
            'mailing_city': 'Provider Business Mailing Address City Name',
            'mailing_state': 'Provider Business Mailing Address State Name',
            'mailing_zip_code': 'Provider Business Mailing Address Postal Code',
            'mailing_country': 'Provider Business Mailing Address Country Code (If outside U.S.)',
            'mailing_phone': 'Provider Business Mailing Address Telephone Number',
            'mailing_fax': 'Provider Business Mailing Address Fax Number',
            'practice_line_1': 'Provider First Line Business Practice Location Address',
            'practice_line_2': 'Provider Second Line Business Practice Location Address',
            'practice_city': 'Provider Business Practice Location Address City Name',
            'practice_state': 'Provider Business Practice Location Address State Name',
            'practice_zip_code': 'Provider Business Practice Location Address Postal Code',
            'practice_country': 'Provider Business Practice Location Address Country Code (If outside U.S.)',
            'practice_phone': 'Provider Business Practice Location Address Telephone Number',
            'practice_fax': 'Provider Business Practice Location Address Fax Number',
            'npi_enumeration_date': 'Provider Enumeration Date',
            'last_update_date': 'Last Update Date',
            'certification_date': 'Certification Date',
            'deactivation_date': 'NPI Deactivation Date',
            'reactivation_date': 'NPI Reactivation Date',
            'deactivation_reason': 'NPI Deactivation Reason Code',
            'is_sole_proprietor': 'Is Sole Proprietor',
            'is_organization_subpart': 'Is Organization Subpart'
        }
    
    def map(self, df: DataFrame, type_id):
        """
        Map DataFrame to JSON structure based on type_id
        
        Args:
            df: DataFrame containing provider data
            type_id: 'CMS' or 'NPI' to determine mapping schema
            
        Returns:
            list: List of JSON objects for each row in DataFrame
        """
        if type_id.upper() == 'CMS':
            return self._map_cms_data(df)
        elif type_id.upper() == 'NPI':
            return self._map_npi_data(df)
        else:
            raise ValueError("type_id must be 'CMS' or 'NPI'")
    
    def _map_cms_data(self, df: DataFrame):
        """Map CMS DataFrame to JSON structure"""
        results = []
        
        for _, row in df.iterrows():
            # Get secondary specialties
            secondary_specialties = []
            for i in range(1, 5):
                spec_col = f'sec_spec_{i}'
                if spec_col in df.columns and pd.notna(row.get(spec_col)):
                    secondary_specialties.append(row[spec_col])
            
            # Add sec_spec_all if available
            if 'sec_spec_all' in df.columns and pd.notna(row.get('sec_spec_all')):
                secondary_specialties.append(row['sec_spec_all'])
            
            provider_data = {}
            # Provider Identification
            provider_data.update(self._provider_identification(
                npi = self._get_column_value(row,'NPI'),
                pac_id=self._get_column_value(row, 'Ind_PAC_ID'),
                enrollment_id=self._get_column_value(row, 'Ind_enrl_ID')
            ))
            
            # Provider Personal Info
            provider_data.update(self._provider_personal_info(
                last_name=self._get_column_value(row, 'Provider Last Name'),
                first_name=self._get_column_value(row, 'Provider First Name'),
                middle_name=self._get_column_value(row, 'Provider Middle Name'),
                suffix=self._get_column_value(row, 'suff'),
                gender=self._get_column_value(row, 'gndr'),
                credentials=self._get_column_value(row, 'Cred')
            ))
            
            # Provider Professional Info
            provider_data.update(self._provider_professional_info(
                medical_school=self._get_column_value(row, 'Med_sch'),
                graduation_year=self._get_column_value(row, 'Grd_yr'),
                primary_specialty=self._get_column_value(row, 'pri_spec'),
                secondary_specialties=secondary_specialties
            ))
            
            # Current Practice Info
            provider_data.update(self._current_practice_info(
                facility_name=self._get_column_value(row, 'Facility Name'),
                facility_pac_id=self._get_column_value(row, 'org_pac_id'),
                organization_members_count=self._get_column_value(row, 'num_org_mem'),
                practice_address_line_1=self._get_column_value(row, 'adr_ln_1'),
                practice_address_line_2=self._get_column_value(row, 'adr_ln_2'),
                practice_address_city=self._get_column_value(row, 'City/Town'),
                practice_address_state=self._get_column_value(row, 'State'),
                practice_address_zip_code=self._get_column_value(row, 'ZIP Code'),
                practice_address_phone=self._get_column_value(row, 'Telephone Number'),
                practice_address_id=self._get_column_value(row, 'adrs_id')
            ))
            
            # Medicare Participation
            provider_data.update(self._medicare_participation(
                individual_assignment=self._get_column_value(row, 'ind_assgn'),
                group_assignment=self._get_column_value(row, 'grp_assgn')
            ))
            
            # Telehealth Services
            provider_data.update(self._telehealth_services(
                telehealth_eligible=self._get_column_value(row, 'Telehlth')
            ))
            
            # Add empty sections for completeness
            provider_data.update(self._provider_licensing())
            provider_data.update(self._business_addresses())
            provider_data.update(self._provider_status())
            provider_data.update(self._additional_identifiers())
            provider_data.update(self._authorized_official())
            provider_data.update(self._parent_organization())
            
            json_str = json.dumps(provider_data, sort_keys=True)
            
            data_hash = hashlib.sha256(json_str.encode()).hexdigest()
            
            provider_data.update(self._meta_info(data_hash))
            
            results.append(provider_data)
        
        return results
    
    def _map_npi_data(self, df: DataFrame):
        """Map NPI DataFrame to JSON structure"""
        results = []
        
        for _, row in df.iterrows():
            provider_data = {}
            
            taxonomy_code = []
            for i in range(1, 7):
                tax_col = f'Healthcare Provider Taxonomy Code_{i}'
                if tax_col in df.columns and pd.notna(row.get(tax_col)):
                    taxonomy_code.append(row[tax_col])
            
            # Provider Identification
            provider_data.update(self._provider_identification(
                npi=self._get_column_value(row, 'NPI'),
                entity_type_code=self._get_column_value(row, 'Entity Type Code')
            ))
            
            # Provider Personal Info
            provider_data.update(self._provider_personal_info(
                last_name=self._get_column_value(row, 'Provider Last Name (Legal Name)'),
                first_name=self._get_column_value(row, 'Provider First Name'),
                middle_name=self._get_column_value(row, 'Provider Middle Name'),
                suffix=self._get_column_value(row, 'Provider Name Suffix Text'),
                gender=self._get_column_value(row, 'Provider Sex Code'),
                credentials=self._get_column_value(row, 'Provider Credential Text')
            ))
            # Provider Professional Info
            provider_data.update(self._provider_professional_info(
                taxonomy_code=taxonomy_code,
                taxonomy_primary=self._get_column_value(row, 'Healthcare Provider Primary Taxonomy Switch_1')
            ))
            
            # Provider Licensing
            provider_data.update(self._provider_licensing(
                license_number=self._get_column_value(row, 'Provider License Number_1'),
                license_state=self._get_column_value(row, 'Provider License Number State Code_1')
            ))
            
            # Business Addresses
            mailing_data = {
                'line_1': self._get_column_value(row, 'Provider First Line Business Mailing Address'),
                'line_2': self._get_column_value(row, 'Provider Second Line Business Mailing Address'),
                'city': self._get_column_value(row, 'Provider Business Mailing Address City Name'),
                'state': self._get_column_value(row, 'Provider Business Mailing Address State Name'),
                'zip_code': self._get_column_value(row, 'Provider Business Mailing Address Postal Code'),
                'country': self._get_column_value(row, 'Provider Business Mailing Address Country Code (If outside U.S.)'),
                'phone': self._get_column_value(row, 'Provider Business Mailing Address Telephone Number'),
                'fax': self._get_column_value(row, 'Provider Business Mailing Address Fax Number')
            }
            
            practice_data = {
                'line_1': self._get_column_value(row, 'Provider First Line Business Practice Location Address'),
                'line_2': self._get_column_value(row, 'Provider Second Line Business Practice Location Address'),
                'city': self._get_column_value(row, 'Provider Business Practice Location Address City Name'),
                'state': self._get_column_value(row, 'Provider Business Practice Location Address State Name'),
                'zip_code': self._get_column_value(row, 'Provider Business Practice Location Address Postal Code'),
                'country': self._get_column_value(row, 'Provider Business Practice Location Address Country Code (If outside U.S.)'),
                'phone': self._get_column_value(row, 'Provider Business Practice Location Address Telephone Number'),
                'fax': self._get_column_value(row, 'Provider Business Practice Location Address Fax Number')
            }
            
            provider_data.update(self._business_addresses(
                mailing_data=mailing_data if any(mailing_data.values()) else None,
                practice_data=practice_data if any(practice_data.values()) else None
            ))
            
            # Provider Status
            active = self._get_column_value(row, 'NPI Deactivation Date') is None
            provider_data.update(self._provider_status(
                active=active,
                deactivation_reason=self._get_column_value(row, 'NPI Deactivation Reason Code'),
                is_sole_proprietor=self._get_column_value(row, 'Is Sole Proprietor'),
                is_organization_subpart=self._get_column_value(row, 'Is Organization Subpart')
            ))
            
            # Add empty sections for completeness
            provider_data.update(self._current_practice_info())
            provider_data.update(self._medicare_participation())
            provider_data.update(self._telehealth_services())
            provider_data.update(self._additional_identifiers())
            provider_data.update(self._authorized_official())
            provider_data.update(self._parent_organization())
            
            json_str = json.dumps(provider_data, sort_keys=True)
            
            data_hash = hashlib.sha256(json_str.encode()).hexdigest()
            
            provider_data.update(self._meta_info(data_hash=data_hash))
            
            results.append(provider_data)
        
        return results
    
    def _get_column_value(self, row, column_name):
        """Safely get column value, return None if column doesn't exist or value is NaN"""
        try:
            import pandas as pd
            if column_name in row.index:
                value = row[column_name]
                return value if pd.notna(value) else None
            return None
        except Exception:
            return None
    
    def _provider_identification(self, npi=None, pac_id=None, enrollment_id=None, entity_type_code=None):
        return { "provider_identification": {
            "npi": npi,
            "pac_id": pac_id,
            "enrollment_id": enrollment_id,
            "entity_type_code": entity_type_code 
        }}
    
    def _provider_personal_info(self, last_name=None, first_name=None, middle_name=None, suffix=None, gender=None, credentials=None):
        return {"provider_personal_info": {
            "last_name": last_name,
            "first_name": first_name,
            "middle_name": middle_name,
            "suffix": suffix,
            "gender": gender,
            "credentials": credentials
        }}

    def _provider_professional_info(self, medical_school=None, graduation_year=None, primary_specialty=None, secondary_specialties=None, taxonomy_code=None, taxonomy_primary=None):
        
        return {"provider_professional_info": {
            "medical_school": medical_school,
            "graduation_year": graduation_year,
            "primary_specialty": primary_specialty,
            "secondary_specialties": secondary_specialties if secondary_specialties else [],
            "taxonomy_code": taxonomy_code,
            "taxonomy_primary": taxonomy_primary
        }}
    
    def _provider_licensing(self, license_number=None, license_state=None):
        return {"provider_licensing": {
            "license_number": license_number,
            "license_state": license_state
        }}
    
    def _business_addresses(self, mailing_data=None, practice_data=None):
        return {"business_addresses": {
            "mailing_address": self._mailing_address(**mailing_data) if mailing_data else None,
            "practice_location": self._practice_location(**practice_data) if practice_data else None
        }}
    
    def _mailing_address(self, line_1=None, line_2=None, city=None, state=None, zip_code=None, country=None, phone=None, fax=None):
        return {"mailing_address": {
            "line_1": line_1,
            "line_2": line_2,
            "city": city,
            "state": state,
            "zip_code": zip_code,
            "country": country,
            "phone": phone,
            "fax": fax
        }}
    
    def _practice_location(self, line_1=None, line_2=None, city=None, state=None, zip_code=None, country=None, phone=None, fax=None):
        return {"practice_location": {
            "line_1": line_1,
            "line_2": line_2,
            "city": city,
            "state": state,
            "zip_code": zip_code,
            "country": country,
            "phone": phone,
            "fax": fax
        }}
    
    def _current_practice_info(self, facility_name=None, facility_pac_id=None, organization_members_count=None, 
                              practice_address_line_1=None, practice_address_line_2=None, practice_address_city=None,
                              practice_address_state=None, practice_address_zip_code=None, practice_address_phone=None,
                              practice_address_id=None):
        return {"current_practice_info": {
            "facility_name": facility_name,
            "facility_pac_id": facility_pac_id,
            "organization_members_count": organization_members_count,
            "practice_address": {
                "line_1": practice_address_line_1,
                "line_2": practice_address_line_2,
                "city": practice_address_city,
                "state": practice_address_state,
                "zip_code": practice_address_zip_code,
                "phone": practice_address_phone,
                "address_id": practice_address_id
            }
        }}
    
    def _medicare_participation(self, individual_assignment=None, group_assignment=None):
        return {"medicare_participation": {
            "individual_assignment": individual_assignment,
            "group_assignment": group_assignment
        }}
    
    def _provider_status(self, active=None, deactivation_reason=None, is_sole_proprietor=None, is_organization_subpart=None):
        return {"provider_status": {
            "active": active,
            "deactivation_reason": deactivation_reason,
            "is_sole_proprietor": is_sole_proprietor,
            "is_organization_subpart": is_organization_subpart
        }}
    
    def _telehealth_services(self, telehealth_eligible=None):
        return {"telehealth_services": {
            "telehealth_eligible": telehealth_eligible
        }}
    
    def _additional_identifiers(self, identifiers=None):
        return {"additional_identifiers": identifiers if identifiers else []}
    
    def _authorized_official(self, last_name=None, first_name=None, middle_name=None, title=None, phone=None, credentials=None):
        return {"authorized_official": {
            "last_name": last_name,
            "first_name": first_name,
            "middle_name": middle_name,
            "title": title,
            "phone": phone,
            "credentials": credentials
        }}
    
    def _parent_organization(self, legal_business_name=None, tax_id=None):
        return {"parent_organization": {
            "legal_business_name": legal_business_name,
            "tax_id": tax_id
        }}
    def _meta_info(self,data_hash=None):
        return { "meta_info": {
        "data_hash": data_hash,
        "last_update": datetime.now().isoformat()
      }}