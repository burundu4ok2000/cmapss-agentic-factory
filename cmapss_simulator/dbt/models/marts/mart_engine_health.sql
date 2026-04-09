-- mart_engine_health.sql
select * from {{ ref('stg_telemetry') }}
