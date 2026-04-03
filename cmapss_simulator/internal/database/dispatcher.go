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

// ==============================================================================
// ДИСПЕТЧЕР ВОЗДУШНОГО ДВИЖЕНИЯ (State Orchestrator)
// AI-Ready: Lock-Free архитектура (CSP), Batch-оптимизация I/O.
// Интеграция с Chaos Engineering (Zombie State Attack).
// ==============================================================================

// DispatcherConfig — структура для чистой передачи настроек.
type DispatcherConfig struct {
	DBPath          string
	SimulationMode  string 
	MinDurationSec  int
	MaxDurationSec  int
	IdleMinSec      int 
	IdleMaxSec      int 
	AnchorTime      time.Time
	BatchSize       int
	FlushIntervalMs int
}

// Dispatcher — монопольный владелец соединения с SQLite. Оркестрирует I/O без мьютексов.
type Dispatcher struct {
	db          *sql.DB
	TakeoffChan chan TakeoffRequest
	LandingChan chan LandingReport
	config      DispatcherConfig
	unitClocks  map[int32]time.Time
	wg          sync.WaitGroup
	
	// flushInterceptor позволяет Хаос-Обезьянам перехватывать процесс сброса 
	// логов на диск (например, для атаки Zombie State).
	flushInterceptor func(batch []LandingReport)
}

func NewDispatcher(cfg DispatcherConfig) (*Dispatcher, error) {
	// 🚨 ПАРАНОЙЯ L1: Sanity Check конфига
	if cfg.MinDurationSec <= 0 || cfg.MaxDurationSec <= 0 || cfg.MinDurationSec > cfg.MaxDurationSec {
		return nil, fmt.Errorf("FATAL: Невалидные тайминги полета")
	}

	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("ошибка открытия БД: %w", err)
	}

	// 🚨 ПАРАНОЙЯ L2: Запрет мультиплексирования (один коннект к файлу для избежания Database is locked)
	db.SetMaxOpenConns(1)

	// 🚨 ПАРАНОЙЯ L3: WAL режим + Autocheckpoint (Спасение SSD от переполнения журнала)
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

	// 🚨 ПАРАНОЙЯ L4: Recovery (Санитарная очистка брошенных полетов после краша)
	res, err := db.Exec("UPDATE flights SET status = 'PENDING' WHERE status IN ('IN_FLIGHT', 'INTERRUPTED')")
	if err != nil {
		return nil, fmt.Errorf("ошибка Recovery при обновлении статусов: %w", err)
	}
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected > 0 {
		log.Printf("[DISPATCHER] Внимание: Выполнено восстановление %d прерванных рейсов обратно в PENDING.", rowsAffected)
	}

	d := &Dispatcher{
		db:          db,
		TakeoffChan: make(chan TakeoffRequest, cfg.BatchSize),
		LandingChan: make(chan LandingReport, cfg.BatchSize),
		config:      cfg,
		unitClocks:  make(map[int32]time.Time),
	}
	
	// По умолчанию интерцептор просто вызывает базовый метод записи
	d.flushInterceptor = d.defaultFlushLandings

	return d, nil
}

// GetDB возвращает указатель на БД для Оркестратора Хаоса (Zombie State Attack).
func (d *Dispatcher) GetDB() *sql.DB {
	return d.db
}

// SetLandingInterceptor позволяет внедрить Middleware для саботажа процесса посадки.
// Это инъекция хаоса уровня "Sauron" (CMMS Split Brain).
func (d *Dispatcher) SetLandingInterceptor(interceptor func(context.Context, LandingReport, func(context.Context, []LandingReport))) {
	d.flushInterceptor = func(batch []LandingReport) {
		for _, rep := range batch {
			interceptor(context.Background(), rep, func(ctx context.Context, r []LandingReport) {
				d.defaultFlushLandings(r)
			})
		}
	}
}

func (d *Dispatcher) Start(ctx context.Context) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.runEventLoop(ctx)
	}()
}

// Wait блокирует вызывающую горутину, пока Диспетчер полностью не остановится.
func (d *Dispatcher) Wait() {
	d.wg.Wait()
}

// runEventLoop — сердце Диспетчера. Обеспечивает Lock-Free доступ к БД через каналы (CSP).
func (d *Dispatcher) runEventLoop(ctx context.Context) {
	// Пред-аллокация памяти (Zero-Allocation паттерн) для батчинга
	landingBatch := make([]LandingReport, 0, d.config.BatchSize)

	flushTicker := time.NewTicker(time.Duration(d.config.FlushIntervalMs) * time.Millisecond)
	defer flushTicker.Stop()

	log.Println("[DISPATCHER] Служба управления воздушным движением запущена.")

	for {
		select {
		case <-ctx.Done():
			// 🚨 ПАРАНОЙЯ L5: Graceful Shutdown
			if len(landingBatch) > 0 {
				d.flushInterceptor(landingBatch)
			}
			d.db.Close()
			log.Println("[DISPATCHER] Остановлен. БД безопасно закрыта.")
			return

		case req := <-d.TakeoffChan:
			d.handleTakeoff(req)

		case report := <-d.LandingChan:
			landingBatch = append(landingBatch, report)
			if len(landingBatch) >= d.config.BatchSize {
				d.flushInterceptor(landingBatch)
				landingBatch = landingBatch[:0]
			}

		case <-flushTicker.C:
			if len(landingBatch) > 0 {
				d.flushInterceptor(landingBatch)
				landingBatch = landingBatch[:0]
			}
		}
	}
}

// handleTakeoff ищет следующий полет и бронирует его транзакцией.
func (d *Dispatcher) handleTakeoff(req TakeoffRequest) {
	var rec FlightRecord
	var _id int 
	var _status string
	var _start, _dur sql.NullString

	// Используем точное перечисление колонок во избежание проблем с SELECT * при добавлении новых колонок
	query := `SELECT 
		id, status, flight_start_time, target_duration_sec,
		unit_number, time_cycles, op_setting_1, op_setting_2, op_setting_3,
		T2, T24, T30, T50, P2, P15, P30, Nf, Nc, epr, Ps30, phi, NRf, NRc,
		BPR, farB, htBleed, Nf_dmd, PCNfR_dmd, W31, W32
		FROM flights 
		WHERE unit_number = ? AND status = 'PENDING' 
		ORDER BY time_cycles ASC LIMIT 1`

	row := d.db.QueryRow(query, req.UnitNumber)

	// Сканируем результаты строго в соответствии с автосгенерированной структурой FlightRecord
	err := row.Scan(
		&_id, &_status, &_start, &_dur,
		&rec.UnitNumber, &rec.TimeCycles,
		&rec.OpSetting1, &rec.OpSetting2, &rec.OpSetting3,
		&rec.T2, &rec.T24, &rec.T30, &rec.T50, &rec.P2, &rec.P15, &rec.P30,
		&rec.Nf, &rec.Nc, &rec.Epr, &rec.Ps30, &rec.Phi, &rec.Nrf, &rec.Nrc,
		&rec.Bpr, &rec.Farb, &rec.Htbleed, &rec.NfDmd, &rec.PcnfrDmd, &rec.W31, &rec.W32,
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

	// Машина состояний времени (Watermarking anchor)
	if d.config.SimulationMode == "REALTIME" {
		startTime = time.Now().UTC()
	} else {
		lastTime, exists := d.unitClocks[req.UnitNumber]
		if !exists {
			lastTime = d.config.AnchorTime
		}
		idleSec := rand.IntN(d.config.IdleMaxSec-d.config.IdleMinSec+1) + d.config.IdleMinSec
		startTime = lastTime.Add(time.Duration(idleSec) * time.Second)
		// Сохраняем время окончания этого полета как якорь для следующего
		d.unitClocks[req.UnitNumber] = startTime.Add(time.Duration(durationSec) * time.Second)
	}

	startTimeStr := startTime.Format(time.RFC3339)

	// Бронируем рейс (чтобы другая горутина его не забрала)
	updateQ := `UPDATE flights SET status = 'IN_FLIGHT', flight_start_time = ?, target_duration_sec = ? WHERE id = ?`
	_, err = d.db.Exec(updateQ, startTimeStr, durationSec, _id)
	if err != nil {
		log.Printf("[DISPATCHER] Ошибка бронирования рейса Unit-%d (ID:%d): %v", req.UnitNumber, _id, err)
		req.RespChan <- TakeoffResponse{HasMoreFlights: false} 
		return
	}

	// Выдаем "Разрешение на взлет" борту
	req.RespChan <- TakeoffResponse{
		HasMoreFlights:    true,
		Record:            rec, 
		StartTime:         startTime,
		TargetDurationSec: durationSec,
	}
}

// defaultFlushLandings — оригинальная логика сброса логов (без хаоса).
func (d *Dispatcher) defaultFlushLandings(batch []LandingReport) {
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