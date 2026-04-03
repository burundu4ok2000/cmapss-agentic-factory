package flight

import (
	"context"
	"log"
	"sync"
	"time"

	"cmapss_simulator/internal/arinc429"
	"cmapss_simulator/internal/database"
	"cmapss_simulator/internal/domain"
	"cmapss_simulator/internal/physics"
)

// ==============================================================================
// EDGE COMPUTING WORKER (Autonomous Pilot / Lambda 2.0 Gateway)
// AI-Ready Standard:
// 1. DO-178C Isolation (Защита от Starvation, Deadlocks и Panic).
// 2. ACARS Hybrid Routing: 100 Hz QAR (Disk) + 1 Hz ARINC 429 Satcom (Stream).
// 3. Интеграция с Chaos Engineering (QarInterceptor, PhysicalInjector через domain).
// ==============================================================================

const (
	// 🚨 ПАРАНОЙЯ (Timeouts): Жесткие лимиты на ожидание ответа от инфраструктуры.
	dispatcherTimeout = 5 * time.Second
	backoffDuration   = 2 * time.Second
)

// WorkerConfig передается каждому пилоту при старте.
type WorkerConfig struct {
	SimulationMode string
	TargetHz       int
	Compression    int
	OutputDir      string
	RunID          string
	
	// Инъекции Хаоса из "Подвала" (Dependency Inversion)
	SatcomBroker     domain.StreamBroker   // Интерфейс потоковой передачи (Edge-to-Cloud)
	PhysicalInjector domain.ChaosInjector  // Инъектор разрушения датчиков (Валинор)
	
	// QarInterceptor — Middleware для перехвата записи на диск (Zstd-Бомба)
	QarInterceptor func(ctx context.Context, rec database.FlightRecord, startTime time.Time, durationSec int, runID string, status string, cfg *physics.SimulatorConfig, saveFunc func(rec database.FlightRecord, startTime time.Time, durationSec int, runID string, status string, cfg physics.SimulatorConfig) error) error
}

// Pilot — автономная горутина, управляющая жизненным циклом одной турбины.
// 🚨 ПАРАНОЙЯ L1 (Isolation): Горутина полностью инкапсулирована. 
// Ее паника или зависание никогда не обрушит соседние борта.
func Pilot(unitID int32, ctx context.Context, wg *sync.WaitGroup, d *database.Dispatcher, m *SharedMetrics, cfg WorkerConfig) {
	defer wg.Done()

	// 🚨 ПАРАНОЙЯ L2 (Defensive Recover): Перехват паники (например, деление на ноль в физике).
	// Гарантирует, что WaitGroup отщелкнется, и система сможет выключиться (Graceful Shutdown).
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[PILOT-%d] ⚠️ FATAL PANIC: %v", unitID, r)
			m.Errors.Add(1)
		}
	}()

	m.ActivePilots.Add(1)
	defer m.ActivePilots.Add(-1)

	// Хелпер для безопасного сохранения данных (пропускает через Zstd-Обезьяну, если она есть)
	saveData := func(rec database.FlightRecord, st time.Time, dur int, rid string, stat string, simCfg physics.SimulatorConfig) error {
		if cfg.QarInterceptor != nil {
			return cfg.QarInterceptor(ctx, rec, st, dur, rid, stat, &simCfg, physics.SimulateAndSave)
		}
		return physics.SimulateAndSave(rec, st, dur, rid, stat, simCfg)
	}

	for {
		respChan := make(chan database.TakeoffResponse, 1)
		req := database.TakeoffRequest{
			UnitNumber: unitID,
			RespChan:   respChan,
		}

		// --- ФАЗА 1: ЗАПРОС ВЗЛЕТА (Circuit Breaker) ---
		select {
		case <-ctx.Done():
			return // Сигнал на остановку завода
		case d.TakeoffChan <- req:
			// Запрос успешно отправлен
		case <-time.After(dispatcherTimeout):
			// 🚨 ПАРАНОЙЯ L4 (Starvation Protection): Диспетчер перегружен (SQLite Lock).
			// Не висим бесконечно. Логируем Warning, ждем (Backoff) и пробуем снова.
			log.Printf("[PILOT-%d] WARNING: Timeout отправки запроса. Диспетчер перегружен. Backoff...", unitID)
			time.Sleep(backoffDuration)
			continue
		}

		// --- ФАЗА 2: ОЖИДАНИЕ ОТВЕТА (Timeout Guard) ---
		var resp database.TakeoffResponse
		select {
		case <-ctx.Done():
			return
		case resp = <-respChan:
			// Ответ получен
		case <-time.After(dispatcherTimeout):
			// Диспетчер взял запрос, но база "зависла" на выполнении SELECT/UPDATE.
			log.Printf("[PILOT-%d] WARNING: Timeout ответа от Диспетчера. Возможен Database Lock. Backoff...", unitID)
			time.Sleep(backoffDuration)
			continue
		}

		// Если циклы для турбины закончились — Пилот выходит на пенсию
		if !resp.HasMoreFlights {
			log.Printf("[PILOT-%d] Нет доступных рейсов (RUL=0 / Взрыв турбины). Конец работы.", unitID)
			return
		}

		simCfg := physics.SimulatorConfig{
			TargetHz:         cfg.TargetHz,
			CompressionLevel: cfg.Compression,
			DataOutputDir:    cfg.OutputDir,
		}

		// --- ФАЗА 3: LAMBDA 2.0 МАШИНА СОСТОЯНИЙ (СИМУЛЯЦИЯ + СТРИМИНГ) ---
		if cfg.SimulationMode == "ACCELERATED" {
			// 📡 Спутниковый стриминг (Fast-Forward эмуляция)
			if cfg.SatcomBroker != nil {
				for s := 0; s < resp.TargetDurationSec; s++ {
					tickTime := resp.StartTime.Add(time.Duration(s) * time.Second)
					frame := captureArincFrame(resp.Record, tickTime)
					_ = cfg.SatcomBroker.Transmit(ctx, frame)
				}
			}

			// 💽 Тяжелый батч (QAR) с перехватом Zstd-Бомбы
			err := saveData(resp.Record, resp.StartTime, resp.TargetDurationSec, cfg.RunID, "COMPLETED", simCfg)

			status := "COMPLETED"
			if err != nil {
				log.Printf("[PILOT-%d] ⚠️ Ошибка симуляции Parquet: %v", unitID, err)
				status = "FAILED" 
				m.Errors.Add(1)
			} else {
				m.TotalFlights.Add(1)
				m.FilesSaved.Add(1)
				m.PointsGenerated.Add(uint64(resp.TargetDurationSec * cfg.TargetHz))
			}
			sendSafeReport(ctx, d, unitID, resp.Record.TimeCycles, status)

		} else {
			// Режим REALTIME: Паттерн "Memory Shield"
			timer := time.NewTimer(time.Duration(resp.TargetDurationSec) * time.Second)
			
			// 📡 1 Hz Downsampling Ticker (Спутниковый канал очень дорогой)
			acarsTicker := time.NewTicker(1 * time.Second)

			flightLoop:
			for {
				select {
				case <-ctx.Done():
					// 🚨 ПАРАНОЙЯ L3 (Smart Interruption & Legal Hold)
					timer.Stop()
					acarsTicker.Stop()
					actualSec := int(time.Since(resp.StartTime).Seconds())

					if actualSec > 0 {
						_ = saveData(resp.Record, resp.StartTime, actualSec, cfg.RunID, "INTERRUPTED", simCfg)
						sendSafeReport(ctx, d, unitID, resp.Record.TimeCycles, "INTERRUPTED")
						m.FilesSaved.Add(1)
					}
					return // Штатно завершаем горутину

				case tickTime := <-acarsTicker.C:
					// 📡 Отправка ARINC-кадра раз в секунду
					if cfg.SatcomBroker != nil {
						frame := captureArincFrame(resp.Record, tickTime)
						// Fire-and-forget передача. В реальном мире спутник может не ответить.
						// Именно эту логику будет абьюзить AcarsBufferMonkey.
						_ = cfg.SatcomBroker.Transmit(ctx, frame)
					}

				case <-timer.C:
					acarsTicker.Stop()
					// 💽 Посадка. Сброс тяжелого 100 Гц Parquet батча.
					err := saveData(resp.Record, resp.StartTime, resp.TargetDurationSec, cfg.RunID, "COMPLETED", simCfg)
					
					status := "COMPLETED"
					if err != nil {
						status = "FAILED"
						m.Errors.Add(1)
					} else {
						m.TotalFlights.Add(1)
						m.FilesSaved.Add(1)
						m.PointsGenerated.Add(uint64(resp.TargetDurationSec * cfg.TargetHz))
					}
					sendSafeReport(ctx, d, unitID, resp.Record.TimeCycles, status)
					break flightLoop // Выход из внутреннего цикла, переход к следующему рейсу
				}
			}
		}
	}
}

// captureArincFrame берет идеальный Baseline флоатов и пропускает их через
// горнило Lossy Compression (Квантования), упаковывая в 32-битные слова ARINC 429.
// Возвращает структуру domain.ARINCFrame (Контракт из Подвала).
func captureArincFrame(rec database.FlightRecord, ts time.Time) domain.ARINCFrame {
	var words []arinc429.Word
	
	// SDI (Source/Destination Identifier) - занимает 2 бита (0-3). 
	// Используем остаток от деления для привязки к номеру двигателя на крыле.
	sdi := uint8(rec.UnitNumber % 4) 

	// Вспомогательное замыкание для чистой упаковки (DRY)
	encode := func(sensorName string, val float64) {
		if cfg, exists := arinc429.SensorRegistry[sensorName]; exists {
			words = append(words, arinc429.EncodeBNR(cfg, sdi, val, arinc429.SsmBnrNormalOp))
		}
	}

	// Упаковываем критические метрики для ACARS трансляции
	encode("T2", rec.T2)
	encode("T50", rec.T50) // Критический маркер EGT Margin
	encode("P30", rec.P30) // Маркер обледенения трубки Пито
	encode("Nf", rec.Nf)   // Ground Truth якорь
	encode("Nc", rec.Nc)
	encode("htBleed", rec.Htbleed) // Маркер для Radiation Bit-Flip атаки

	return domain.ARINCFrame{
		Timestamp:  ts,
		UnitNumber: rec.UnitNumber,
		TimeCycles: rec.TimeCycles,
		Words:      words,
	}
}

// sendSafeReport гарантирует доставку рапорта даже при выключении (Cascade Shutdown).
func sendSafeReport(ctx context.Context, d *database.Dispatcher, unitID, cycles int32, status string) {
	rep := database.LandingReport{
		UnitNumber: unitID,
		TimeCycles: cycles,
		Status:     status,
	}
	select {
	case <-ctx.Done():
		// 🚨 ПАРАНОЙЯ L6 (Shutdown Best-Effort): Контекст мертв, но мы обязаны попытаться
		// записать статус (чтобы база не осталась в вечном IN_FLIGHT). Таймаут 1 сек.
		select {
		case d.LandingChan <- rep:
		case <-time.After(1 * time.Second):
			log.Printf("[PILOT-%d] ⚠️ Дроп рапорта (Диспетчер не отвечает при Shutdown)", unitID)
		}
	case d.LandingChan <- rep:
	}
}