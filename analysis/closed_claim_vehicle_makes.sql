with closed_claims as (
  select
    c.claim_id,
    c.policy_id,
    c.report_date,
    c.incident_date,
    c.claim_cause,
    c.loss_amount,
    c.total_payout_amount,
    c.outstanding_amount,
    p.vehicle_vin,
    v.make as vehicle_make,
    v.model as vehicle_model,
    v.vehicle_type
  from {{ ref('fct_claims') }} c
  join {{ ref('dim_policy') }} p
    on p.policy_id = c.policy_id
  join {{ ref('dim_vehicle') }} v
    on v.vehicle_vin = p.vehicle_vin
  where c.claim_status = 'closed'
)
select * from closed_claims

