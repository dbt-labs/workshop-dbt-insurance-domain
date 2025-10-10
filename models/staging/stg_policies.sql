with source as (
  select * from {{ ref('policies') }}
),
typed as (
  select
    cast(policy_id as varchar) as policy_id,
    trim(policy_number) as policy_number,
    cast(policyholder_key as varchar) as policyholder_id,
    cast(agency_key as varchar) as agency_id,
    cast(product_id as varchar) as product_id,
    trim(vehicle_key) as vehicle_vin,
    cast(start_date as date) as start_date,
    cast(end_date as date) as end_date,
    lower(trim(policy_status)) as policy_status
  from source
)
select * from typed


