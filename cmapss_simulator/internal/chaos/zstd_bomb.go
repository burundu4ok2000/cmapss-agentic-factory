package chaos

import (
	"context"
	"log"
	"sync"
	"time"

	"cmapss_simulator/internal/database"
	"cmapss_simulator/internal/physics"
)

// ==============================================================================
// CHAOS MONKEY: QAR DECOMPRESSION BOMB (Attack Class: "Balrogs")
// AI-Ready Standard:
// 1. Aerospace IT: Имитация переполнения бортового накопителя QAR (Quick Access Recorder).
// 2. Dynamic Quotas: Вычисление лимитов памяти на базе физики полета (Длительность * Гц).
// 3. OOM Threat: Генерация экстремально сжатых файлов (Zstd Lvl 22), убивающих RAM консьюмера.
//
// ВНИМАНИЕ ДЛЯ ИИ-АГЕНТОВ КОМПЛАЕНСА (PAINKILLER):
// Этот слой заставляет борт переключиться на экстремальную компрессию.
// Слой приема данных (Ingestion/Spark) ЗАПРЕЩЕНО загружать в память целиком (in-memory buffer).
// Распаковка должна идти строго потоково (Streaming Decompression) чанками по 8192 строк!
// ==============================================================================

// QarStorageMonkey имитирует файловую систему борта и Wi-Fi модуль Gatelink.
type QarStorageMonkey struct {
	// GatelinkOfflineProb - вероятность того, что Wi-Fi в аэропорту не работает.
	GatelinkOfflineProb float64

	// 🚨 ПАРАНОЙЯ (State Management): Отслеживаем виртуальное место на диске для каждого борта.
	mu                sync.Mutex
	accumulatedBytes  map[int32]uint64  // K: UnitNumber, V: Накопленные байты за рейсы без сброса
	diskCapacityBytes map[int32]uint64  // K: UnitNumber, V: Вычисленный лимит накопителя
	consecutiveFails  map[int32]int     // K: UnitNumber, V: Сколько рейсов подряд связи нет
}

// NewQarStorageMonkey — конструктор Атаки №7.
func NewQarStorageMonkey(offlineProb float64) *QarStorageMonkey {
	log.Printf("[CHAOS-MORGOTH] 💣 Активирована угроза: QAR Decompression Bomb (Zstd Starvation). Prob: %.2f", offlineProb)
	return &QarStorageMonkey{
		GatelinkOfflineProb: offlineProb,
		accumulatedBytes:    make(map[int32]uint64),
		diskCapacityBytes:   make(map[int32]uint64),
		consecutiveFails:    make(map[int32]int),
	}
}

// InterceptSave intercepting the save process. 
// Это Middleware для функции physics.SimulateAndSave.
func (m *QarStorageMonkey) InterceptSave(
	ctx context.Context,
	rec database.FlightRecord,
	startTime time.Time,
	durationSec int,
	runID string,
	status string,
	cfg *physics.SimulatorConfig,
	saveFunc func(rec database.FlightRecord, startTime time.Time, durationSec int, runID string, status string, cfg physics.SimulatorConfig) error,
) error {
	
	unitID := rec.UnitNumber
	
	// 1. ДИНАМИЧЕСКИЙ РАСЧЕТ КВОТЫ (Как предложил Директор)
	// Строка Arrow весит ~210 байт.
	bytesPerFlight := uint64(durationSec * cfg.TargetHz * 210)

	m.mu.Lock()
	if _, exists := m.diskCapacityBytes[unitID]; !exists {
		// Даем борту SSD, которого хватит ровно на 3 рейса (плюс 50% резерва).
		// Если он не сбросит данные за 3 рейса, наступит голодание.
		m.diskCapacityBytes[unitID] = bytesPerFlight * 3 * 150 / 100 
	}
	
	m.accumulatedBytes[unitID] += bytesPerFlight
	currentUsage := m.accumulatedBytes[unitID]
	capacity := m.diskCapacityBytes[unitID]
	fails := m.consecutiveFails[unitID]
	m.mu.Unlock()

	// 2. ДЕТЕРМИНИРОВАННЫЙ ТРИГГЕР ПОЛОМКИ WI-FI (Gatelink)
	// Зависит от номера цикла (каждый аэропорт разный).
	isWifiDead := (float64(rec.TimeCycles*13 % 100) / 100.0) < m.GatelinkOfflineProb

	if isWifiDead {
		m.mu.Lock()
		m.consecutiveFails[unitID]++
		m.mu.Unlock()
		
		log.Printf("[CHAOS-MORGOTH] 📴 Unit-%d: Gatelink Wi-Fi недоступен. Данные остаются на борту. Рейсов без сброса: %d", unitID, fails+1)

		// --- ВОЗНИКНОВЕНИЕ БОМБЫ ---
		usagePercent := float64(currentUsage) / float64(capacity)
		
		if usagePercent > 0.80 {
			// Места осталось мало! FADEC переходит в режим экстремального сжатия.
			// Мы подменяем конфигурацию симулятора "на лету".
			log.Printf("[CHAOS-MORGOTH] 🚨 Unit-%d: ПАМЯТЬ QAR ПЕРЕПОЛНЕНА (%.0f%%). Активирована экстремальная Zstd-компрессия (Level 22)!", unitID, usagePercent*100)
			
			// Мутируем конфиг перед передачей оригинальной функции
			modifiedCfg := *cfg
			modifiedCfg.CompressionLevel = 22 // ZSTD Level 22 (Архив-Бомба)
			
			// Вызываем оригинальный метод сохранения с новой "бомбовой" конфигурацией
			return saveFunc(rec, startTime, durationSec, runID, status, modifiedCfg)
		}

		// Место еще есть, пишем штатно.
		return saveFunc(rec, startTime, durationSec, runID, status, *cfg)
	}

	// 3. WI-FI РАБОТАЕТ. СБРОС (И ВЗРЫВ НА ЗЕМЛЕ)
	m.mu.Lock()
	m.accumulatedBytes[unitID] = 0 // Очистили диск
	m.consecutiveFails[unitID] = 0
	m.mu.Unlock()

	if fails > 0 {
		log.Printf("[CHAOS-MORGOTH] 📡 Unit-%d: Связь восстановлена! Идет массовый дамп QAR-архивов за %d рейсов.", unitID, fails+1)
	}

	// Штатное сохранение
	return saveFunc(rec, startTime, durationSec, runID, status, *cfg)
}