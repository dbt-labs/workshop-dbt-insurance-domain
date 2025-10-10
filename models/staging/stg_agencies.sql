with source as (
  select * from {{ ref('agencies') }}
),
renamed as (
  select
    cast(agency_key as varchar) as agency_id,
    trim(name) as agency_name,
    trim(region) as region,
    lower(trim(email)) as email,
    trim(phone) as phone,
    initcap(trim(channel_type)) as channel_type
  from source
)
select * from renamed


