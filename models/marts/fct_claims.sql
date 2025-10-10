with claims as (
  select * from {{ ref('stg_claims') }}
),
payouts as (
  select claim_id, sum(payout_amount) as total_payout_amount
  from {{ ref('stg_claim_payouts') }}
  group by 1
),
joined as (
  select
    c.claim_id,
    c.policy_id,
    c.report_date,
    c.incident_date,
    c.claim_status,
    c.claim_cause,
    c.loss_amount,
    coalesce(p.total_payout_amount, 0) as total_payout_amount,
    c.loss_amount - coalesce(p.total_payout_amount, 0) as outstanding_amount
  from claims c
  left join payouts p on c.claim_id = p.claim_id
)
select * from joined

