package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"cmapss_simulator/internal/database"
	"cmapss_simulator/internal/flight"
	"cmapss_simulator/internal/ui"

	"github.com/joho/godotenv"
	"github.com/pterm/pterm"
)

// getEnvAsInt — хелпер для безопасного парсинга .env
func getEnvAsInt(key string, fallback int) int {
	if val, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallback
}

func main() {
	// 1. BOOTSTRAPPING & ENV
	if err := godotenv.Load("configs/.env"); err != nil {
		pterm.Warning.Println("Файл configs/.env не найден. Используются переменные окружения ОС.")
	}

	runID := fmt.Sprintf("run_%s", time.Now().Format("20060102_150405"))
	logPath := os.Getenv("LOG_FILE_PATH")
	if logPath == "" {
		logPath = "data/simulator.log"
	}

	// 🚨 ПАРАНОЙЯ L1 (Logger Shield): Перехват логов до старта UI
	logFile, err := ui.InitLogger(logPath)
	if err != nil {
		pterm.Fatal.Printf("Не удалось инициализировать логер: %v\n", err)
	}
	defer logFile.Close()
	log.Printf("=== START SIMULATOR RUN: %s ===", runID)

	// Чтение конфигурации с фолбэками
	anchorTime, _ := time.Parse(time.RFC3339, os.Getenv("SIMULATION_EPOCH"))
	poolSize := getEnvAsInt("WORKER_POOL_SIZE", 100)

	dispCfg := database.DispatcherConfig{
		DBPath:          os.Getenv("DATABASE_URL"), // Будет пусто, если нет в .env, берем хардкод ниже
		SimulationMode:  os.Getenv("SIMULATION_MODE"),
		MinDurationSec:  getEnvAsInt("FLIGHT_DURATION_MIN", 120),
		MaxDurationSec:  getEnvAsInt("FLIGHT_DURATION_MAX", 300),
		IdleMinSec:      getEnvAsInt("AIRPORT_IDLE_MIN", 15),
		IdleMaxSec:      getEnvAsInt("AIRPORT_IDLE_MAX", 30),
		AnchorTime:      anchorTime,
		BatchSize:       getEnvAsInt("BATCH_SIZE", 100),
		FlushIntervalMs: getEnvAsInt("FLUSH_INTERVAL_MS", 100),
	}
	if dispCfg.DBPath == "" {
		dispCfg.DBPath = "data/blueprints.sqlite"
	}

	workerCfg := flight.WorkerConfig{
		SimulationMode: dispCfg.SimulationMode,
		TargetHz:       getEnvAsInt("TARGET_HZ", 100),
		Compression:    getEnvAsInt("COMPRESSION_LEVEL", 1),
		OutputDir:      os.Getenv("DATA_OUTPUT_DIR"),
		RunID:          runID,
	}

	// 2. INITIALIZATION
	dispatcher, err := database.NewDispatcher(dispCfg)
	if err != nil {
		pterm.Fatal.Printf("Ошибка запуска Диспетчера: %v\n", err)
	}

	metrics := &flight.SharedMetrics{}

	// 🚨 ПАРАНОЙЯ L2 (Dual-Context Architecture):
	// Разделяем контексты для безопасного каскадного выключения.
	rootCtx, cancelRoot := context.WithCancel(context.Background())
	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	// 3. START COMPONENTS
	dispatcher.Start(rootCtx)
	go ui.StartDashboard(rootCtx, metrics, runID, dispCfg.SimulationMode, getEnvAsInt("UI_REFRESH_RATE_MS", 1000))

	// Запуск пула пилотов (1 пилот = 1 уникальный физический двигатель)
	for i := 1; i <= poolSize; i++ {
		wg.Add(1)
		// Передаем i как UnitNumber (двигатели в NASA пронумерованы от 1 до 100)
		go flight.Pilot(int32(i), workerCtx, wg, dispatcher, metrics, workerCfg)
	}

	// 4. THE GRACEFUL CASCADE (Оркестрация выключения)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan // Главный поток замирает и ждет Ctrl+C

	// TUI перетирается новой строкой для логов
	pterm.DefaultBasicText.Println("\n")
	pterm.Info.Println("[SHUTDOWN] Получен сигнал завершения. Инициирована посадка всех бортов...")
	log.Println("Инициировано каскадное завершение (Graceful Cascade)")

	// Шаг 1. Посылаем сигнал смерти Пилотам.
	cancelWorkers()

	// Шаг 2. Ждем, пока последний активный пилот не сделает wg.Done().
	// Это гарантирует, что C-память Arrow освобождена (Release) и Parquet-футеры записаны!
	wg.Wait()

	// Шаг 3. Убиваем Диспетчера (закроет батчи SQLite).
	cancelRoot()

	// Шаг 4. Ждем, пока Диспетчер безопасно сохранит данные на медленный диск.
    dispatcher.Wait()

	// Пауза для сброса логов заменена на Шаг 4.
	// time.Sleep(500 * time.Millisecond)
	pterm.Success.Println("Завод 4.0 остановлен без потери данных. Bye!")
}