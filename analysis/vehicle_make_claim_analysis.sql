with claims as (
  select
    fc.claim_id,
    fc.policy_id,
    fc.total_payout_amount
  from {{ ref('fct_claims') }} fc
),
policies as (
  select
    p.policy_id,
    p.vehicle_vin
  from {{ ref('dim_policy') }} p
),
vehicles as (
  select
    v.vehicle_vin,
    v.make
  from {{ ref('dim_vehicle') }} v
),
joined as (
  select
    v.make as vehicle_make,
    c.claim_id,
    c.total_payout_amount
  from claims c
  join policies p on c.policy_id = p.policy_id
  join vehicles v on p.vehicle_vin = v.vehicle_vin
)
select
  vehicle_make,
  count(distinct claim_id) as number_of_claims,
  sum(total_payout_amount) as total_payout_amount
from joined
group by vehicle_make
order by total_payout_amount desc, number_of_claims desc



