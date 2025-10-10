select
  vehicle_vin,
  make,
  model,
  year,
  vehicle_type
from {{ ref('stg_vehicles') }}


