package flight

import (
	"context"
	"log"
	"sync"
	"time"

	"cmapss_simulator/internal/database"
	"cmapss_simulator/internal/physics"
)

// StreamBroker — пустой интерфейс-заглушка (Hook) для реализации Этапа 3 в будущем (Redpanda/Kafka).
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
// 🚨 ПАРАНОЙЯ: Изолирована от паник, не блокирует Диспетчера.
func Pilot(unitID int32, ctx context.Context, wg *sync.WaitGroup, d *database.Dispatcher, m *SharedMetrics, cfg WorkerConfig) {
	defer wg.Done()
	
	// 🚨 ПАРАНОЙЯ L1 (Isolation): Перехват паники физики
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[PILOT-%d] FATAL PANIC: %v", unitID, r)
			m.Errors.Add(1)
		}
	}()

	m.ActivePilots.Add(1)
	defer m.ActivePilots.Add(-1)

	for {
		// 🚨 ПАРАНОЙЯ L2 (Deadlock Immunity): Канал ответа строго буферизован (размер 1).
		// Если пилот умрет по Ctrl+C во время ожидания, Диспетчер не зависнет при отправке ответа.
		respChan := make(chan database.TakeoffResponse, 1)
		req := database.TakeoffRequest{
			UnitNumber: unitID,
			RespChan:   respChan,
		}

		// Запрос взлета (с возможностью прерывания)
		select {
		case <-ctx.Done():
			return
		case d.TakeoffChan <- req:
		}

		// Ожидание ответа от Диспетчера
		var resp database.TakeoffResponse
		select {
		case <-ctx.Done():
			return
		case resp = <-respChan:
		}

		// Если циклы для турбины закончились — Пилот выходит на пенсию
		if !resp.HasMoreFlights {
			log.Printf("[PILOT-%d] Нет доступных рейсов (взрыв турбины). Конец работы.", unitID)
			return
		}

		simCfg := physics.SimulatorConfig{
			TargetHz:         cfg.TargetHz,
			CompressionLevel: cfg.Compression,
			DataOutputDir:    cfg.OutputDir,
		}

		// 🚨 ПАРАНОЙЯ L3 (Машина Состояний Времени)
		if cfg.SimulationMode == "ACCELERATED" {
			// Ускоренный режим: Не спим, сразу генерируем Parquet.
			err := physics.SimulateAndSave(resp.Record, resp.StartTime, resp.TargetDurationSec, cfg.RunID, simCfg)
			
			status := "COMPLETED"
			if err != nil {
				log.Printf("[PILOT-%d] Ошибка симуляции Parquet: %v", unitID, err)
				status = "FAILED"
				m.Errors.Add(1)
			} else {
				m.TotalFlights.Add(1)
				m.FilesSaved.Add(1)
				m.PointsGenerated.Add(uint64(resp.TargetDurationSec * cfg.TargetHz))
			}
			sendSafeReport(ctx, d, unitID, resp.Record.TimeCycles, status)

		} else {
			// Режим REALTIME: Паттерн "Memory Shield" (Сон -> Генерация)
			timer := time.NewTimer(time.Duration(resp.TargetDurationSec) * time.Second)
			
			select {
			case <-ctx.Done():
				// 🚨 ПАРАНОЙЯ L4 (Smart Interruption): Прервали по Ctrl+C
				timer.Stop()
				actualSec := int(time.Since(resp.StartTime).Seconds())
				
				// Если самолет успел пролететь хотя бы 1 секунду — сохраняем улики
				if actualSec > 0 {
					_ = physics.SimulateAndSave(resp.Record, resp.StartTime, actualSec, cfg.RunID, simCfg)
					sendSafeReport(ctx, d, unitID, resp.Record.TimeCycles, "INTERRUPTED") // заменил context.Background() на ctx
					m.FilesSaved.Add(1)
				}
				return // Завершаем горутину

			case <-timer.C:
				// Нормальное завершение полета
				err := physics.SimulateAndSave(resp.Record, resp.StartTime, resp.TargetDurationSec, cfg.RunID, simCfg)
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
		// Контекст мертв, делаем неблокирующую попытку отправки (таймаут 1 сек)
		select {
		case d.LandingChan <- rep:
		case <-time.After(1 * time.Second):
			log.Printf("[PILOT-%d] Дроп рапорта (Диспетчер не отвечает)", unitID)
		}
	case d.LandingChan <- rep:
	}
}