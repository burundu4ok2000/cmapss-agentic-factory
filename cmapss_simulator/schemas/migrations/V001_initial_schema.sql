-- ==========================================================
-- АВТОГЕНЕРАЦИЯ ИЗ cmapss_engine_telemetry v1.0.0
-- ДАТА ГЕНЕРАЦИИ: 2026-03-29 13:38:14 UTC
-- ВЛАДЕЛЕЦ: Data_Platform_Team
-- НЕ РЕДАКТИРОВАТЬ ВРУЧНУЮ! ВСЕ ИЗМЕНЕНИЯ ВНОСИТЬ В YAML.
-- ==========================================================

DROP TABLE IF EXISTS flights;

CREATE TABLE flights (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT DEFAULT 'PENDING',
    flight_start_time TEXT,
    target_duration_sec INTEGER,
    unit_number INTEGER NOT NULL,
    time_cycles INTEGER NOT NULL,
    op_setting_1 REAL NOT NULL,
    op_setting_2 REAL NOT NULL,
    op_setting_3 REAL NOT NULL,
    T2 REAL NOT NULL,
    T24 REAL NOT NULL,
    T30 REAL NOT NULL,
    T50 REAL NOT NULL,
    P2 REAL NOT NULL,
    P15 REAL NOT NULL,
    P30 REAL NOT NULL,
    Nf REAL NOT NULL,
    Nc REAL NOT NULL,
    epr REAL NOT NULL,
    Ps30 REAL NOT NULL,
    phi REAL NOT NULL,
    NRf REAL NOT NULL,
    NRc REAL NOT NULL,
    BPR REAL NOT NULL,
    farB REAL NOT NULL,
    htBleed REAL NOT NULL,
    Nf_dmd REAL NOT NULL,
    PCNfR_dmd REAL NOT NULL,
    W31 REAL NOT NULL,
    W32 REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_flights_unit_cycle ON flights(unit_number, time_cycles);