select
  {{ dbt_utils.generate_surrogate_key(['vehicle_vin']) }} as vehicle_id,
  vehicle_vin,
  make,
  model,
  year,
  vehicle_type
from {{ ref('stg_vehicles') }}


