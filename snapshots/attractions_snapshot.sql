{% snapshot attractions_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='attraction_id',
      strategy='timestamp',
      updated_at='load_time',
    )
}}

select * from {{ source('amusement_park', 'attractions') }}

{% endsnapshot %}