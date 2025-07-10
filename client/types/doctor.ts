export interface Doctor {
  _id: string
  provider_identification: {
    npi: number
    pac_id: number | null
    enrollment_id: string | null
    entity_type_code: number
  }
  provider_personal_info: {
    last_name: string
    first_name: string
    middle_name: string | null
    suffix: string | null
    gender: string
    credentials: string
  }
  provider_professional_info: {
    medical_school: string | null
    graduation_year: number | null
    primary_specialty: string | null
    secondary_specialties: string[]
    taxonomy_code: string[]
    taxonomy_primary: string
  }
  provider_licensing: {
    license_number: string | null
    license_state: string | null
  }
  business_addresses: {
    mailing_address: {
      mailing_address: {
        line_1: string
        line_2: string | null
        city: string
        state: string
        zip_code: number
        country: string
        phone: number | null
        fax: number | null
      }
    }
    practice_location: {
      practice_location: {
        line_1: string
        line_2: string | null
        city: string
        state: string
        zip_code: number
        country: string
        phone: number | null
        fax: number | null
      }
    }
  }
  provider_status: {
    active: boolean
    deactivation_reason: string | null
    is_sole_proprietor: string
    is_organization_subpart: string | null
  }
  current_practice_info: {
    facility_name: string | null
    facility_pac_id: number | null
    organization_members_count: number | null
    practice_address: {
      line_1: string | null
      line_2: string | null
      city: string | null
      state: string | null
      zip_code: string | null
      phone: number | null
      address_id: string | null
    }
  }
  medicare_participation: {
    individual_assignment: string | null
    group_assignment: string | null
  }
  telehealth_services: {
    telehealth_eligible: boolean | null
  }
  additional_identifiers: any[]
  authorized_official: {
    last_name: string | null
    first_name: string | null
    middle_name: string | null
    title: string | null
    phone: number | null
    credentials: string | null
  }
  parent_organization: {
    legal_business_name: string | null
    tax_id: string | null
  }
  meta_info: {
    data_hash: string
    last_update: string
  }
}

export interface ApiResponse {
  doctors: Doctor[]
  pagination: {
    currentPage: number
    totalPages: number
    totalDoctors: number
    hasNextPage: boolean
    hasPrevPage: boolean
    pageSize: number
  }
}
