with source as (
  select * from {{ ref('claims') }}
),
typed as (
  select
    cast(claim_key as varchar) as claim_id,
    cast(policy_key as varchar) as policy_id,
    cast(report_date as date) as report_date,
    cast(incident_date as date) as incident_date,
    lower(trim(claim_status)) as claim_status,
    initcap(trim(claim_cause)) as claim_cause,
    cast(loss_amount as decimal(18,2)) as loss_amount
  from source
)
select * from typed
