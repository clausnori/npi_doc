# Provider Data Mapping Schema

## Provider Identification
- **npi** ← NPI.NPI
- **pac_id** ← CMS.Ind_PAC_ID
- **enrollment_id** ← CMS.Ind_enrl_ID
- **entity_type_code** ← NPI.Entity Type Code

## Provider Personal Info
- **last_name** ← NPI.Provider Last Name (Legal Name) | CMS.Provider Last Name
- **first_name** ← NPI.Provider First Name | CMS.Provider First Name
- **middle_name** ← NPI.Provider Middle Name | CMS.Provider Middle Name
- **suffix** ← NPI.Provider Name Suffix Text | CMS.suff
- **gender** ← NPI.Provider Sex Code | CMS.gndr
- **credentials** ← NPI.Provider Credential Text | CMS.Cred

## Provider Professional Info
- **medical_school** ← CMS.Med_sch
- **graduation_year** ← CMS.Grd_yr
- **primary_specialty** ← CMS.pri_spec
- **secondary_specialties** ← CMS.sec_spec_1, sec_spec_2, sec_spec_3, sec_spec_4, sec_spec_all
- **taxonomy_code** ← NPI.Healthcare Provider Taxonomy Code_1
- **taxonomy_primary** ← NPI.Healthcare Provider Primary Taxonomy Switch_1

## Provider Licensing
- **license_number** ← NPI.Provider License Number_1
- **license_state** ← NPI.Provider License Number State Code_1

## Business Addresses - Mailing
- **line_1** ← NPI.Provider First Line Business Mailing Address
- **line_2** ← NPI.Provider Second Line Business Mailing Address
- **city** ← NPI.Provider Business Mailing Address City Name
- **state** ← NPI.Provider Business Mailing Address State Name
- **zip_code** ← NPI.Provider Business Mailing Address Postal Code
- **country** ← NPI.Provider Business Mailing Address Country Code (If outside U.S.)
- **phone** ← NPI.Provider Business Mailing Address Telephone Number
- **fax** ← NPI.Provider Business Mailing Address Fax Number

## Business Addresses - Practice Location
- **line_1** ← NPI.Provider First Line Business Practice Location Address
- **line_2** ← NPI.Provider Second Line Business Practice Location Address
- **city** ← NPI.Provider Business Practice Location Address City Name
- **state** ← NPI.Provider Business Practice Location Address State Name
- **zip_code** ← NPI.Provider Business Practice Location Address Postal Code
- **country** ← NPI.Provider Business Practice Location Address Country Code (If outside U.S.)
- **phone** ← NPI.Provider Business Practice Location Address Telephone Number
- **fax** ← NPI.Provider Business Practice Location Address Fax Number

## Current Practice Info
- **facility_name** ← CMS.Facility Name
- **facility_pac_id** ← CMS.org_pac_id
- **organization_members_count** ← CMS.num_org_mem
- **practice_address.line_1** ← CMS.adr_ln_1
- **practice_address.line_2** ← CMS.adr_ln_2
- **practice_address.city** ← CMS.City/Town
- **practice_address.state** ← CMS.State
- **practice_address.zip_code** ← CMS.ZIP Code
- **practice_address.phone** ← CMS.Telephone Number
- **practice_address.address_id** ← CMS.adrs_id

## Medicare Participation
- **individual_assignment** ← CMS.ind_assgn
- **group_assignment** ← CMS.grp_assgn

## Administrative Dates
- **npi_enumeration_date** ← NPI.Provider Enumeration Date
- **last_update_date** ← NPI.Last Update Date
- **certification_date** ← NPI.Certification Date
- **deactivation_date** ← NPI.NPI Deactivation Date
- **reactivation_date** ← NPI.NPI Reactivation Date

## Provider Status
- **active** ← computed from NPI.NPI Deactivation Date
- **deactivation_reason** ← NPI.NPI Deactivation Reason Code
- **is_sole_proprietor** ← NPI.Is Sole Proprietor
- **is_organization_subpart** ← NPI.Is Organization Subpart

## Telehealth Services
- **telehealth_eligible** ← CMS.Telehlth

## Additional Fields (Empty Arrays/Objects)
- **additional_identifiers** ← NPI.Other Provider Identifier_1-50
- **authorized_official** ← NPI.Authorized Official fields
- **parent_organization** ← NPI.Parent Organization fields