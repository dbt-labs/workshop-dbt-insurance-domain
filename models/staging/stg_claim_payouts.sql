with source as (
  select * from {{ ref('claim_payouts') }}
),
typed as (
  select
    cast(claim_key as varchar) as claim_id,
    cast(payout_key as varchar) as payout_id,
    cast(payout_date as date) as payout_date,
    cast(payout_amount as decimal(18,2)) as payout_amount,
    initcap(trim(payout_type)) as payout_type
  from source
)
select * from typed