package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	// Подключаем CGO-free драйвер SQLite (AI-Ready standard)
	_ "modernc.org/sqlite"
)

// DispatcherConfig — структура для чистой передачи настроек (избавление от длинных списков аргументов).
type DispatcherConfig struct {
	DBPath          string
	SimulationMode  string // "REALTIME" или "ACCELERATED"
	MinDurationSec  int
	MaxDurationSec  int
	IdleMinSec      int // new: 
	IdleMaxSec      int // для имитации простоя между полетами
	AnchorTime      time.Time
	BatchSize       int
	FlushIntervalMs int
}

// Dispatcher — монопольный владелец SQLite. Оркестрирует конкурентный I/O.
type Dispatcher struct {
	db          *sql.DB
	TakeoffChan chan TakeoffRequest
	LandingChan chan LandingReport
	config      DispatcherConfig

	// 🚨 ПАРАНОЙЯ: Логические часы двигателей для режима ACCELERATED.
	// Гарантируют непрерывность времени между полетами без коллизий.
	unitClocks  map[int32]time.Time
	
	wg          sync.WaitGroup // NEW: Внутренний счетчик Диспетчера
}

// NewDispatcher подключается к БД, валидирует конфигурацию и делает Recovery.
func NewDispatcher(cfg DispatcherConfig) (*Dispatcher, error) {
	// 🚨 ПАРАНОЙЯ: Sanity Check (Защита от паники rand.IntN)
	if cfg.MinDurationSec <= 0 || cfg.MaxDurationSec <= 0 || cfg.MinDurationSec > cfg.MaxDurationSec {
		return nil, fmt.Errorf("FATAL: Невалидные тайминги полета (min=%d, max=%d)", cfg.MinDurationSec, cfg.MaxDurationSec)
	}

	if cfg.IdleMinSec < 0 || cfg.IdleMaxSec < 0 || cfg.IdleMinSec > cfg.IdleMaxSec {
    return nil, fmt.Errorf("FATAL: Невалидные тайминги простоя (min=%d, max=%d)", cfg.IdleMinSec, cfg.IdleMaxSec)
	}

	if cfg.BatchSize <= 0 || cfg.FlushIntervalMs <= 0 {
		return nil, fmt.Errorf("FATAL: Невалидные настройки батчинга (size=%d, interval=%d)", cfg.BatchSize, cfg.FlushIntervalMs)
	}

	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("ошибка открытия БД: %w", err)
	}

	// 🚨 ПАРАНОЙЯ L1: Запрет мультиплексирования (один коннект к файлу)
	db.SetMaxOpenConns(1)

	// 🚨 ПАРАНОЙЯ L2: WAL режим + Autocheckpoint (Спасение SSD от переполнения журнала)
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA wal_autocheckpoint=1000;",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return nil, fmt.Errorf("ошибка применения PRAGMA %s: %w", p, err)
		}
	}

	// 🚨 ПАРАНОЙЯ L3: Recovery (Санитарная очистка брошенных полетов)
	res, err := db.Exec("UPDATE flights SET status = 'PENDING' WHERE status IN ('IN_FLIGHT', 'INTERRUPTED')")
	if err != nil {
		return nil, fmt.Errorf("ошибка Recovery при обновлении статусов: %w", err)
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected > 0 {
		log.Printf("[DISPATCHER] Внимание: Выполнено восстановление %d прерванных рейсов обратно в PENDING.", rowsAffected)
	}

	return &Dispatcher{
		db:          db,
		TakeoffChan: make(chan TakeoffRequest, cfg.BatchSize), // Буфер канала зависит от конфига
		LandingChan: make(chan LandingReport, cfg.BatchSize),
		config:      cfg,
		unitClocks:  make(map[int32]time.Time), //new: Инициализация логических часов для каждого двигателя
	}, nil
}

// Start запускает Event Loop в фоновой горутине. NEW
func (d *Dispatcher) Start(ctx context.Context) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done() // Гарантирует, что Wait() разблокируется только после db.Close()
		d.runEventLoop(ctx)
	}()
}

// Wait блокирует вызывающую горутину, пока Диспетчер полностью не остановится.
func (d *Dispatcher) Wait() {
    d.wg.Wait()
}


// runEventLoop — сердце Диспетчера. Обеспечивает Lock-Free доступ к БД.
func (d *Dispatcher) runEventLoop(ctx context.Context) {
	// Пред-аллокация памяти (Zero-Allocation паттерн)
	landingBatch := make([]LandingReport, 0, d.config.BatchSize)
	
	flushTicker := time.NewTicker(time.Duration(d.config.FlushIntervalMs) * time.Millisecond)
	defer flushTicker.Stop()

	log.Println("[DISPATCHER] Служба управления воздушным движением запущена.")

	for {
		select {
		case <-ctx.Done():
			// 🚨 ПАРАНОЙЯ L4: Graceful Shutdown
			if len(landingBatch) > 0 {
				d.flushLandings(landingBatch)
			}
			d.db.Close()
			log.Println("[DISPATCHER] Остановлен. БД безопасно закрыта.")
			return

		case req := <-d.TakeoffChan:
			d.handleTakeoff(req)

		case report := <-d.LandingChan:
			landingBatch = append(landingBatch, report)
			if len(landingBatch) >= d.config.BatchSize {
				d.flushLandings(landingBatch)
				landingBatch = landingBatch[:0] // Очистка среза без утечек памяти
			}

		case <-flushTicker.C:
			if len(landingBatch) > 0 {
				d.flushLandings(landingBatch)
				landingBatch = landingBatch[:0]
			}
		}
	}
}

// handleTakeoff ищет следующий полет и бронирует его транзакцией.
func (d *Dispatcher) handleTakeoff(req TakeoffRequest) {
	var rec FlightRecord
	var _status string
	var _start, _dur sql.NullString

	query := `SELECT * FROM flights WHERE unit_number = ? AND status = 'PENDING' ORDER BY time_cycles ASC LIMIT 1`
	row := d.db.QueryRow(query, req.UnitNumber)

	err := row.Scan(
		&_status, &_start, &_dur, // 3 системные
		&rec.UnitNumber, &rec.TimeCycles, // 2 идентификатора
		&rec.OpSetting1, &rec.OpSetting2, &rec.OpSetting3, // 3 настройки
		&rec.T2, &rec.T24, &rec.T30, &rec.T50, &rec.P2, &rec.P15, &rec.P30, // Датчики 1
		&rec.Nf, &rec.Nc, &rec.Epr, &rec.Ps30, &rec.Phi, &rec.NRf, &rec.NRc, // Датчики 2
		&rec.BPR, &rec.FarB, &rec.HtBleed, &rec.Nf_dmd, &rec.PCNfR_dmd, &rec.W31, &rec.W32, // Датчики 3
	)

	if err == sql.ErrNoRows {
		req.RespChan <- TakeoffResponse{HasMoreFlights: false}
		return
	} else if err != nil {
		log.Printf("[DISPATCHER] Ошибка чтения рейса для Unit-%d: %v", req.UnitNumber, err)
		req.RespChan <- TakeoffResponse{HasMoreFlights: false}
		return
	}

	durationSec := rand.IntN(d.config.MaxDurationSec-d.config.MinDurationSec+1) + d.config.MinDurationSec

	var startTime time.Time

	// 🚨 ПАРАНОЙЯ L5: Машина состояний времени. NEW
	if d.config.SimulationMode == "REALTIME" {
		startTime = time.Now().UTC()
	} else {
		lastTime, exists := d.unitClocks[req.UnitNumber]
		if !exists {
			lastTime = d.config.AnchorTime
		}
		idleSec := rand.IntN(d.config.IdleMaxSec-d.config.IdleMinSec+1) + d.config.IdleMinSec
		startTime = lastTime.Add(time.Duration(idleSec) * time.Second)
		d.unitClocks[req.UnitNumber] = startTime.Add(time.Duration(durationSec) * time.Second)
	}

	startTimeStr := startTime.Format(time.RFC3339)

	updateQ := `UPDATE flights SET status = 'IN_FLIGHT', flight_start_time = ?, target_duration_sec = ? WHERE unit_number = ? AND time_cycles = ?`
	_, err = d.db.Exec(updateQ, startTimeStr, durationSec, rec.UnitNumber, rec.TimeCycles)
	if err != nil {
		log.Printf("[DISPATCHER] Ошибка бронирования рейса Unit-%d: %v", req.UnitNumber, err)
		req.RespChan <- TakeoffResponse{HasMoreFlights: false} // <-- Отменяем старт для воркера
		return // <-- Обязательно прерываем функцию, иначе воркер получит данные, но рейс не будет забронирован, что приведет к коллизиям и неконсистентности данных.
	}

	req.RespChan <- TakeoffResponse{
		HasMoreFlights:    true,
		Record:            rec,
		StartTime:         startTime,
		TargetDurationSec: durationSec,
	}
}

// flushLandings делает массовый UPDATE (Батчинг) для спасения диска Asus.
func (d *Dispatcher) flushLandings(batch []LandingReport) {
	tx, err := d.db.Begin()
	if err != nil {
		log.Printf("[DISPATCHER] Ошибка старта транзакции батча: %v", err)
		return
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`UPDATE flights SET status = ? WHERE unit_number = ? AND time_cycles = ?`)
	if err != nil {
		log.Printf("[DISPATCHER] Ошибка подготовки стейтмента: %v", err)
		return
	}
	defer stmt.Close()

	for _, rep := range batch {
		if _, err := stmt.Exec(rep.Status, rep.UnitNumber, rep.TimeCycles); err != nil {
			log.Printf("[DISPATCHER] Ошибка апдейта Unit-%d: %v", rep.UnitNumber, err)
			continue
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[DISPATCHER] Ошибка коммита батча посадок: %v", err)
	}
}