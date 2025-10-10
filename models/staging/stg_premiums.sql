with source as (
  select * from {{ ref('premiums') }}
),
typed as (
  select
    cast(policy_key as varchar) as policy_id,
    cast(effective_date as date) as effective_date,
    cast(gross_premium_amount as decimal(18,2)) as gross_premium_amount,
    cast(fees as decimal(18,2)) as fees,
    cast(discounts as decimal(18,2)) as discounts,
    cast(net_premium_amount as decimal(18,2)) as net_premium_amount
  from source
)
select * from typed


