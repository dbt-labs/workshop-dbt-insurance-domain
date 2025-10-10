select
  policy_id,
  effective_date,
  gross_premium_amount,
  fees,
  discounts,
  net_premium_amount
from {{ ref('stg_premiums') }}


