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
    -- Normalize phone to (XXX) XXX-XXXX (strip non-digits, drop leading 1)
    (
      '(' || substr(
        case
          when length(regexp_replace(trim(phone), '[^0-9]', '')) = 11
               and left(regexp_replace(trim(phone), '[^0-9]', ''), 1) = '1'
            then substr(regexp_replace(trim(phone), '[^0-9]', ''), 2, 10)
          else regexp_replace(trim(phone), '[^0-9]', '')
        end, 1, 3
      ) || ') ' || substr(
        case
          when length(regexp_replace(trim(phone), '[^0-9]', '')) = 11
               and left(regexp_replace(trim(phone), '[^0-9]', ''), 1) = '1'
            then substr(regexp_replace(trim(phone), '[^0-9]', ''), 2, 10)
          else regexp_replace(trim(phone), '[^0-9]', '')
        end, 4, 3
      ) || '-' || substr(
        case
          when length(regexp_replace(trim(phone), '[^0-9]', '')) = 11
               and left(regexp_replace(trim(phone), '[^0-9]', ''), 1) = '1'
            then substr(regexp_replace(trim(phone), '[^0-9]', ''), 2, 10)
          else regexp_replace(trim(phone), '[^0-9]', '')
        end, 7, 4
      )
    ) as phone,
    trim(address) as address,
    trim(city) as city,
    upper(trim(state)) as state,
    trim(zip) as zip
  from source
)
select * from renamed


