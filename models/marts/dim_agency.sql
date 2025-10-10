select
  agency_id,
  agency_name,
  region,
  email,
  phone,
  channel_type
from {{ ref('stg_agencies') }}


