

  create or replace view `de-zoomcamp-2026-485615`.`cmapss_telemetry`.`stg_telemetry`
  OPTIONS()
  as with source_data as (
    select * from `de-zoomcamp-2026-485615`.`cmapss_telemetry`.`raw_telemetry`
),

refined_telemetry as (
    select
        -- Filtering corrupted data at the staging level
        unit_number,
        time_cycles,
        timestamp as event_timestamp,
        
        -- Physical sensors
        T2,
        T50,
        P30,
        Nf,
        Nc,
        phi,
        htBleed,

        -- Derived analytics
        (1450 - T50) as egt_margin,
        
        -- Metadata
        is_corrupted,
        corruption_reason

    from source_data
    where is_corrupted = false
)

select * from refined_telemetry;

