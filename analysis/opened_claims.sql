with recent_open_claims as (
  select
    claim_id,
    policy_id,
    report_date,
    incident_date,
    claim_status,
    claim_cause,
    loss_amount,
    total_payout_amount,
    outstanding_amount
  from {{ ref('fct_claims') }}
  where claim_status = 'open'
    and report_date >= dateadd(day, -30, current_date())
)
select * from recent_open_claims

