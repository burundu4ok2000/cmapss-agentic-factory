package physics

import (
	"fmt"
	"path/filepath"
	"time"

	"cmapss_simulator/internal/database"
	"cmapss_simulator/internal/storage"
)

// SimulatorConfig передается из основной точки входа для управления Фазой 4.
type SimulatorConfig struct {
	TargetHz         int
	CompressionLevel int
	DataOutputDir    string
}

// SimulateAndSave — единая точка входа для горутины-Слота.
// AI-Ready: Инкапсулирует сложную логику управления памятью (Reference Counting Arrow) и I/O.
func SimulateAndSave(rec database.FlightRecord, startTime time.Time, durationSec int, runID string, cfg SimulatorConfig) error {
	totalRows := durationSec * cfg.TargetHz

	// 1. Инициализация Памяти
	// Reserve() выделит память монолитом. defer rb.Release() спасет от утечек.
	rb := storage.NewRecordBuilder(totalRows)
	defer rb.Release() // 🚨 ПАРАНОЙЯ: Очистка C-аллокатора при любом исходе функции.

	// 2. Симуляция Физики (Наполнение памяти)
	err := GenerateTelemetry(rb.GetBuilder(), rec, startTime, durationSec, cfg.TargetHz, runID)
	if err != nil {
		return fmt.Errorf("сбой физического движка: %w", err)
	}

	// 3. Формирование неизменяемого Arrow Record
	// ВНИМАНИЕ: NewRecord() инкрементирует внутренний счетчик ссылок C-памяти.
	record := rb.GetBuilder().NewRecordBatch()
	defer record.Release() // 🚨 ПАРАНОЙЯ L4: Обязательное освобождение памяти итогового батча!

	// 4. Дата-Озеро (Hive Partitioning)
	// Используем startTime.UTC(), чтобы Spark не словил Timezone Poisoning.
	dateStr := startTime.UTC().Format("2006-01-02")

	// Пример: data/telemetry/run_id=20260321_120000/unit=1/date=2026-01-01/flight_45.parquet
	targetPath := filepath.Join(cfg.DataOutputDir,
		fmt.Sprintf("run_id=%s", runID),
		fmt.Sprintf("unit=%d", rec.UnitNumber),
		fmt.Sprintf("date=%s", dateStr),
		fmt.Sprintf("flight_%d.parquet", rec.TimeCycles),
	)

	// 5. Безопасный I/O (Atomic Write + Fsync + Parquet ZSTD)
	if err := storage.WriteParquetAtomically(record, targetPath, cfg.CompressionLevel); err != nil {
		return fmt.Errorf("критический сбой записи на диск: %w", err)
	}

	return nil
}
