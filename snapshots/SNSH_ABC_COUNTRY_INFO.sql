{% snapshot SNSH_ABC_COUNTRY_INFO %}

{{
   config(
      unique_key = 'COUNTRY_NAME_HKEY',

      strategy='check',

      check_cols=['COUNTRY_HDIFF'],
   )


}}

select * from {{ref('STG_ABC_COUNTRY_INFO')}}

{% endsnapshot %}