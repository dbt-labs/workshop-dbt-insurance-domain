with p as (
  select * from {{ ref('stg_policies') }}
),
joined as (
  select
    p.policy_id,
    p.policy_number,
    p.policyholder_id,
    p.agency_id,
    p.product_id,
    p.vehicle_vin,
    p.start_date,
    p.end_date,
    p.policy_status
  from p
)
select * from joined


