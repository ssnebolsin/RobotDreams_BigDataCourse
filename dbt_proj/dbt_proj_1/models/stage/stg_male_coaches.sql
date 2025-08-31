{{ config(
    materialized='table',
    tags=['stage', 'male', 'coaches']
) }}

with source as (
    select * from {{ source('public', 'male_coaches') }}
)

select * from source
where dob is not null --clear empty values