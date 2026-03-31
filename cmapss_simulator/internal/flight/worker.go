package flight

import (
	"context"
	"log"
	"sync"
	"time"

	"cmapss_simulator/internal/database"
	"cmapss_simulator/internal/physics"
)

// ==============================================================================
// EDGE COMPUTING WORKER (Autonomous Pilot)
// AI-Ready Standard:
// 1. Zero Trust к инфраструктуре (Защита от Starvation и Deadlocks).
// 2. Интеграция с Data Governance (Маршрутизация прерванных рейсов в Карантин).
// ==============================================================================

const (
	// 🚨 ПАРАНОЙЯ (Timeouts): Жесткие лимиты на ожидание ответа от базы.
	dispatcherTimeout = 5 * time.Second
	backoffDuration   = 2 * time.Second
)

// StreamBroker — пустой интерфейс-заглушка (Hook) для реализации Этапа 3 в будущем.
type StreamBroker interface{}

// WorkerConfig передается каждому пилоту при старте.
type WorkerConfig struct {
	SimulationMode string
	TargetHz       int
	Compression    int
	OutputDir      string
	RunID          string
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

	for {
		// 🚨 ПАРАНОЙЯ L3 (Deadlock Immunity): Канал ответа строго буферизован (1).
		// Если пилот умрет по таймауту/Ctrl+C, Диспетчер не зависнет при попытке положить ответ.
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

		// --- ФАЗА 3: МАШИНА СОСТОЯНИЙ (СИМУЛЯЦИЯ) ---
		if cfg.SimulationMode == "ACCELERATED" {
			// Ускоренный режим: сразу генерируем Parquet (статус по умолчанию COMPLETED)
			err := physics.SimulateAndSave(resp.Record, resp.StartTime, resp.TargetDurationSec, cfg.RunID, "COMPLETED", simCfg)

			status := "COMPLETED"
			if err != nil {
				log.Printf("[PILOT-%d] ⚠️ Ошибка симуляции Parquet: %v", unitID, err)
				status = "FAILED" // Будет перенаправлен в Quarantine на следующем рейсе (если выживет)
				m.Errors.Add(1)
			} else {
				m.TotalFlights.Add(1)
				m.FilesSaved.Add(1)
				m.PointsGenerated.Add(uint64(resp.TargetDurationSec * cfg.TargetHz))
			}
			sendSafeReport(ctx, d, unitID, resp.Record.TimeCycles, status)

		} else {
			// Режим REALTIME: Паттерн "Memory Shield" (Сон -> Затем генерация файла)
			timer := time.NewTimer(time.Duration(resp.TargetDurationSec) * time.Second)

			select {
			case <-ctx.Done():
				// 🚨 ПАРАНОЙЯ L5 (Smart Interruption & Legal Hold):
				// Нас прервали по Ctrl+C посреди полета (Аварийная остановка системы).
				timer.Stop()
				actualSec := int(time.Since(resp.StartTime).Seconds())

				// Если борт успел пролететь хотя бы 1 секунду — сохраняем "Черный ящик"
				if actualSec > 0 {
					// Явно передаем статус "INTERRUPTED". Physics Engine направит этот файл в /quarantine/
					_ = physics.SimulateAndSave(resp.Record, resp.StartTime, actualSec, cfg.RunID, "INTERRUPTED", simCfg)
					
					// Отправляем рапорт в БД, чтобы CDC/Spark знали, что рейс прерван
					sendSafeReport(ctx, d, unitID, resp.Record.TimeCycles, "INTERRUPTED")
					m.FilesSaved.Add(1)
				}
				return // Штатно завершаем горутину

			case <-timer.C:
				// Нормальное завершение полета
				err := physics.SimulateAndSave(resp.Record, resp.StartTime, resp.TargetDurationSec, cfg.RunID, "COMPLETED", simCfg)
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
			}
		}
	}
}

// sendSafeReport гарантирует, что Воркер попытается отправить рапорт,
// даже если контекст Воркера уже отменен, но Диспетчер еще жив (Graceful Shutdown Cascade).
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