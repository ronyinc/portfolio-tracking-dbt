{{ config(materialized='ephemeral') }}

WITH 

src_data as (

    SELECT
        
           COUNTRY_NAME,
           COUNTRY_CODE_2_LETTER  as COUNTRY_TWO_LETTER_CODE,
           COUNTRY_CODE_3_LETTER as COUNTRY_THREE_LETTER_CODE,
           COUNTRY_CODE_NUMERIC as COUNTRY_NUMERIC_CODE,
           ISO_3166_2 as COUNTRY_ISO_3166_2_CODE, 
           REGION,
           SUB_REGION,
           INTERMEDIATE_REGION,
           REGION_CODE,
           SUB_REGION_CODE,
           INTERMEDIATE_REGION_CODE, 

           LOAD_TS           as LOAD_TS,          -- TIMESTAMP_NTZ
           'SEED.ABC_COUNTRY_INFO' as RECORD_SOURCE

    FROM {{ source('seeds', 'ABC_COUNTRY_INFO') }}

), 

default_record as 

(

  SELECT 
           'Missing' as COUNTRY_NAME,
           'Missing' as COUNTRY_TWO_LETTER_CODE,
           'Missing' as COUNTRY_THREE_LETTER_CODE,
           '-1' as COUNTRY_NUMERIC_CODE,
           'Missing' as COUNTRY_ISO_3166_2_CODE, 
           'Missing' as REGION,
           'Missing' as SUB_REGION,
           'Missing' as INTERMEDIATE_REGION,
           '-1' as REGION_CODE,
           '-1' as SUB_REGION_CODE,
           '-1' as INTERMEDIATE_REGION_CODE, 

           LOAD_TS           as LOAD_TS,          -- TIMESTAMP_NTZ
           'SEED.ABC_COUNTRY_INFO' as RECORD_SOURCE

    FROM 
         src_data    
),

with_default_record as(

    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record

),

hashed as (

    SELECT
         concat_ws('|', COUNTRY_THREE_LETTER_CODE) as COUNTRY_NAME_HKEY
        ,concat_ws('|', COUNTRY_NAME,COUNTRY_TWO_LETTER_CODE,COUNTRY_THREE_LETTER_CODE,
         CAST(COUNTRY_NUMERIC_CODE as STRING),
         COUNTRY_ISO_3166_2_CODE,
         COALESCE(REGION, '-'),
         COALESCE(SUB_REGION,'-'),
         COALESCE(INTERMEDIATE_REGION,'-'),
         COALESCE(CAST(REGION_CODE as STRING),'-'),
         COALESCE(CAST(SUB_REGION_CODE as STRING),'-'),
         COALESCE(CAST(INTERMEDIATE_REGION_CODE as STRING),'-')
         ) as COUNTRY_HDIFF

        , * EXCLUDE LOAD_TS
        , LOAD_TS as LOAD_TS_UTC
    FROM with_default_record

)

SELECT * FROM hashed



