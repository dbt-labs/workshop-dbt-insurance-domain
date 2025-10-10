with source as (
  select * from {{ ref('policyholders') }}
),
renamed as (
  select
    cast(policyholder_key as varchar) as policyholder_id,
    initcap(trim(first_name)) as first_name,
    initcap(trim(last_name)) as last_name,
    cast(date_of_birth as date) as date_of_birth,
    upper(trim(gender)) as gender,
    lower(trim(email)) as email,
    -- Normalize phone to (XXX) XXX-XXXX
    regexp_replace(regexp_replace(regexp_replace(trim(phone), '[^0-9]', ''), '^(1)?(\d{10})$', '\2'), '^(\d{3})(\d{3})(\d{4})$', '(\1) \2-\3') as phone,
    trim(address) as address,
    trim(city) as city,
    upper(trim(state)) as state,
    trim(zip) as zip
  from source
)
select * from renamed


