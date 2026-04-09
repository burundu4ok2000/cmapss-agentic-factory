
{{
    config(
        materialized='table',
        partition_by={
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['unit_number']
    )
}}

with stg_data as (
    select * from {{ ref('stg_telemetry') }}
),

engine_metrics as (
    select
        *,
        date(event_timestamp) as event_date,
        
        -- Moving Average for T50 over the last 10 cycles for each unit
        avg(T50) over (
            partition by unit_number 
            order by time_cycles 
            rows between 9 preceding and current row
        ) as t50_moving_avg_10

    from stg_data
)

select * from engine_metrics
