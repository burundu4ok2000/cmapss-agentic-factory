package physics

import (
	"fmt"
	"log"
	"path/filepath"
	"time"

	"cmapss_simulator/internal/database"
	"cmapss_simulator/internal/storage"
)

// ==============================================================================
// PHYSICS SIMULATOR ORCHESTRATOR (Memory Shield & Data Governance)
// AI-Ready Standard: 
// 1. Инкапсулирует сложную логику управления C-памятью (Reference Counting Arrow).
// 2. Осуществляет семантическую маршрутизацию данных (Legal Hold).
// ==============================================================================

// SimulatorConfig передается из основной точки входа для управления Фазой 4.
type SimulatorConfig struct {
	TargetHz         int
	CompressionLevel int
	DataOutputDir    string
}

// SimulateAndSave — единая точка входа для горутины-Слота.
// 🚨 ПАРАНОЙЯ L1 (Signature Update): Внедрен параметр flightStatus 
// ("COMPLETED", "INTERRUPTED", "FAILED") для маршрутизации аномалий в Quarantine.
func SimulateAndSave(rec database.FlightRecord, startTime time.Time, durationSec int, runID string, flightStatus string, cfg SimulatorConfig) error {
	totalRows := durationSec * cfg.TargetHz

	// 🚨 ПАРАНОЙЯ L2 (Memory Shield & Observability): Расчет и логирование выделяемой памяти.
	// Для IoT на Edge (Asus с 2 ядрами) внезапная аллокация 1 ГБ памяти убьет процесс (OOM).
	// Вычисляем точный размер: 1 Timestamp (8 байт) + 2 Int32 (8 байт) + 24 Float64 (192 байта) = 208 байт/строка.
	// Добавляем 10% на внутренний оверхед структур Apache Arrow.
	estimatedBytes := float64(totalRows*208) * 1.1
	estimatedMB := estimatedBytes / 1024 / 1024
	log.Printf("[MEMORY SHIELD] Unit-%d (Cycle %d): Запрос на %.2f MB C-памяти для %d строк...", rec.UnitNumber, rec.TimeCycles, estimatedMB, totalRows)

	// 1. Инициализация Памяти
	// Reserve() выделяет память монолитом через C-аллокатор, предотвращая фрагментацию кучи.
	rb := storage.NewRecordBuilder(totalRows)
	
	// 🚨 ПАРАНОЙЯ L3 (Anti-Memory-Leak): Отложенный вызов Release() гарантирует возврат
	// памяти операционной системе даже в случае внезапной `panic` внутри физического движка.
	defer rb.Release()

	// 2. Симуляция Физики (Наполнение памяти)
	err := GenerateTelemetry(rb.GetBuilder(), rec, startTime, durationSec, cfg.TargetHz, runID)
	if err != nil {
		return fmt.Errorf("сбой физического движка: %w", err)
	}

	// 3. Формирование неизменяемого Arrow Record
	// ВНИМАНИЕ: NewRecordBatch() инкрементирует внутренний счетчик ссылок C-памяти.
	record := rb.GetBuilder().NewRecordBatch()
	
	// 🚨 ПАРАНОЙЯ L4: Обязательное освобождение памяти финального батча!
	// Без этого вызова память будет накапливаться с каждым рейсом.
	defer record.Release() 

	// 4. Дата-Озеро (Hive Partitioning)
	// Используем startTime.UTC(), чтобы downstream Spark-кластер не словил Timezone Poisoning.
	dateStr := startTime.UTC().Format("2006-01-02")

	// 🚨 ПАРАНОЙЯ L5 (Data Governance & Legal Hold Routing):
	// По законам FAA/EASA данные сбойных или прерванных турбин должны быть изолированы
	// от чистых данных, чтобы предотвратить отравление ML-моделей (Data Poisoning).
	baseFolder := "telemetry"
	if flightStatus == "FAILED" || flightStatus == "INTERRUPTED" || flightStatus == "QUARANTINE" {
		baseFolder = "quarantine"
		log.Printf("[COMPLIANCE] ⚠️ Unit-%d перенаправлен в зону LEGAL_HOLD (%s). Причина: %s", rec.UnitNumber, baseFolder, flightStatus)
	}

	// Пример генерации пути: data/telemetry/run_id=20260321_120000/unit=1/date=2026-01-01/flight_45.parquet
	targetPath := filepath.Join(cfg.DataOutputDir,
		baseFolder,
		fmt.Sprintf("run_id=%s", runID),
		fmt.Sprintf("unit=%d", rec.UnitNumber),
		fmt.Sprintf("date=%s", dateStr),
		fmt.Sprintf("flight_%d.parquet", rec.TimeCycles),
	)

	// 5. Безопасный I/O (Atomic Write + Fsync + Parquet)
	if err := storage.WriteParquetAtomically(record, targetPath, cfg.CompressionLevel); err != nil {
		return fmt.Errorf("критический сбой записи на диск: %w", err)
	}

	log.Printf("[I/O SUCCESS] Unit-%d: %.2f MB атомарно записаны в %s", rec.UnitNumber, estimatedMB, baseFolder)
	return nil
}
