select
  policyholder_id,
  first_name,
  last_name,
  date_of_birth,
  gender,
  address,
  city,
  state,
  zip
from {{ ref('stg_policyholders') }}


