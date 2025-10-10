select
  product_id,
  product_name,
  coverage_type
from {{ ref('stg_products') }}


