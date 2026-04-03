package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"cmapss_simulator/internal/chaos"
	"cmapss_simulator/internal/database"
	"cmapss_simulator/internal/domain" // Внедрение Подвала
	"cmapss_simulator/internal/flight"
	"cmapss_simulator/internal/ui"

	"github.com/joho/godotenv"
	"github.com/pterm/pterm"
)

// ==============================================================================
// SYSTEM ORCHESTRATOR (Main Entry Point)
// AI-Ready Standard:
// 1. Graceful Shutdown Cascade с жестким таймаутом (Deadman's Switch).
// 2. Изоляция контекстов управления и исполнения.
// 3. Гарантия целостности данных при аварийном выходе.
// ==============================================================================

const (
	// shutdownTimeout — время, отведенное заводу на консервацию при получении SIGTERM.
	// Если за 30 секунд борта не сели, мы выходим жестко, чтобы не блокировать CI/CD.
	shutdownTimeout = 30 * time.Second
)

// getEnvAsInt — Enterprise-хелпер для безопасного извлечения числовых параметров.
func getEnvAsInt(key string, fallback int) int {
	if val, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallback
}

// BlackholeBroker — заглушка для тестирования ACARS-штормов в памяти без реальной Kafka.
// Он молча поглощает ARINC-кадры, позволяя Обезьянам (Chaos Monkeys)
// симулировать перегрузку буферов и задержки.
// 🚨 ИНВЕРСИЯ ЗАВИСИМОСТЕЙ: Используем domain.ARINCFrame вместо flight.ARINCFrame
type BlackholeBroker struct{}

func (b *BlackholeBroker) Transmit(ctx context.Context, frame domain.ARINCFrame) error {
	return nil // Съедает пакет, как черная дыра
}

func main() {
	// --- ШАГ 1: BOOTSTRAPPING (Загрузка конфигурации) ---
	if err := godotenv.Load("configs/.env"); err != nil {
		pterm.Warning.Println("Файл configs/.env не найден. Применяются системные переменные окружения.")
	}

	runID := fmt.Sprintf("run_%s", time.Now().Format("20060102_150405"))
	logPath := os.Getenv("LOG_FILE_PATH")
	if logPath == "" {
		logPath = "data/simulator.log"
	}

	// 🚨 ПАРАНОЙЯ L1 (Logger Shield): Перенаправляем логи ДО запуска UI.
	// Это критично для аудита процесса инициализации.
	logFile, err := ui.InitLogger(logPath)
	if err != nil {
		pterm.Fatal.Printf("CRITICAL: Сбой инициализации системы логирования: %v\n", err)
	}
	defer logFile.Close()

	log.Printf("[SYSTEM] Инициализация Завода 4.0. RunID: %s", runID)

	anchorTime, _ := time.Parse(time.RFC3339, os.Getenv("SIMULATION_EPOCH"))
	poolSize := getEnvAsInt("WORKER_POOL_SIZE", 100)

	// Настройка параметров Диспетчера (State Machine)
	dispCfg := database.DispatcherConfig{
		DBPath:          os.Getenv("DATABASE_URL"),
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

	// --- ШАГ 2: INITIALIZATION (Сборка компонентов) ---
	dispatcher, err := database.NewDispatcher(dispCfg)
	if err != nil {
		log.Printf("[FATAL] Ошибка запуска Диспетчера: %v", err)
		pterm.Fatal.Printf("Ошибка запуска Диспетчера: %v\n", err)
	}

	// 🌋 ИНТЕГРАЦИЯ ХАОСА (MORGOTH'S FORGE) 🌋
	// Читаем профиль хаоса из .env.
	chaosProfile := os.Getenv("CHAOS_PROFILE")

	// В будущем BlackholeBroker будет заменен на реальный Kafka Producer.
	baseBroker := &BlackholeBroker{}

	// Выковываем сборку хаоса (Обезьяны оборачивают базовый брокер)
	chaosAssembler := chaos.BuildChaosRealm(chaosProfile, baseBroker, dispatcher.GetDB())

	// Настройка параметров Пилотов (Edge Devices) с инъекцией зависимостей Хаоса
	workerCfg := flight.WorkerConfig{
		SimulationMode:   dispCfg.SimulationMode,
		TargetHz:         getEnvAsInt("TARGET_HZ", 100),
		Compression:      getEnvAsInt("COMPRESSION_LEVEL", 1),
		OutputDir:        os.Getenv("DATA_OUTPUT_DIR"),
		RunID:            runID,
		SatcomBroker:     chaosAssembler.SatcomBroker,     // Декорированный брокер связи
		PhysicalInjector: chaosAssembler.PhysicalInjector, // Инъектор разрушения датчиков
		QarInterceptor:   chaosAssembler.QarInterceptor,   // Блокировщик записи на SSD (Zstd-Бомба)
	}

	// Если активирована Океаническая Тень (Oceanic Blackout) — оборачиваем каналы Диспетчера.
	if strings.Contains(chaosProfile, "ocean_blackout") || chaosProfile == "BALROGS" || chaosProfile == "APOCALYPSE" {
		// Оборачиваем Диспетчера Обезьяной, вероятность блэкаута 1%, изоляция на 5 минут.
		blackoutMonkey := chaos.NewOceanicBlackoutMonkey(dispatcher, 0.01, 5*time.Minute)
		// Подменяем каналы в Диспетчере так, чтобы Пилоты общались с Обезьяной, а не с БД.
		dispatcher.TakeoffChan = blackoutMonkey.TakeoffChan
		dispatcher.LandingChan = blackoutMonkey.LandingChan
	}

	// Если активирован Парадокс Ремонта (Zombie State) — внедряем хук в метод сброса логов.
	if chaosAssembler.LandingInterceptor != nil {
		dispatcher.SetLandingInterceptor(chaosAssembler.LandingInterceptor)
	}

	metrics := &flight.SharedMetrics{}
	
	// 🚨 ПАРАНОЙЯ L2 (Context Separation):
	// rootCtx — управляет жизненным циклом инфраструктуры (Диспетчер, TUI).
	// workerCtx — управляет только активными полетами.
	rootCtx, cancelRoot := context.WithCancel(context.Background()) 
	workerCtx, cancelWorkers := context.WithCancel(context.Background()) 
	wg := &sync.WaitGroup{}

	// --- ШАГ 3: EXECUTION (Запуск завода) ---
	dispatcher.Start(rootCtx)
	go ui.StartDashboard(rootCtx, metrics, runID, dispCfg.SimulationMode, getEnvAsInt("UI_REFRESH_RATE_MS", 1000))

	// Запуск пула автономных пилотов
	for i := 1; i <= poolSize; i++ {
		wg.Add(1)
		go flight.Pilot(int32(i), workerCtx, wg, dispatcher, metrics, workerCfg)
	}

	log.Printf("[SYSTEM] Пул из %d пилотов запущен. Симуляция в процессе...", poolSize)

	// --- ШАГ 4: THE GRACEFUL CASCADE (Оркестрация выключения) ---
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan // Ждем сигнал прерывания (Ctrl+C)

	pterm.DefaultBasicText.Println("\n")
	pterm.Info.Println("[SHUTDOWN] Получен сигнал прерывания. Запуск каскадной остановки...")
	log.Println("[SHUTDOWN] Инициирован Graceful Shutdown Cascade")

	// 1. Остановка Воркеров (Запрет на новые взлеты + Посадка текущих)
	cancelWorkers()
	log.Println("[SHUTDOWN] Сигнал отмены отправлен всем пилотам.")

	// 🚨 ПАРАНОЙЯ L3 (The Deadman's Switch):
	// Мы создаем канал для ожидания WaitGroup с таймаутом.
	waitChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitChan)
	}()

	select {
	case <-waitChan:
		pterm.Success.Println("[SHUTDOWN] Все борта успешно совершили посадку и сохранили данные.")
		log.Println("[SHUTDOWN] Пилоты завершили работу штатно.")
	case <-time.After(shutdownTimeout):
		pterm.Error.Println("[SHUTDOWN] Превышено время ожидания посадки! Возможна потеря данных в ОЗУ.")
		log.Println("[SHUTDOWN] WARNING: Forced shutdown triggered by timeout.")
	}

	// 2. Остановка инфраструктуры
	// Сначала гасим Диспетчера (он сбросит последние батчи статусов в SQLite)
	cancelRoot()
	dispatcher.Wait()
	log.Println("[SHUTDOWN] Диспетчер БД остановлен. Соединение закрыто.")

	// Финальный аккорд
	pterm.Success.Println("Завод полностью законсервирован. Все ресурсы освобождены.")
	log.Println("=== END SIMULATOR RUN ===")
}