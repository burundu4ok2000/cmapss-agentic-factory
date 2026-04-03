package chaos

import (
	"hash/fnv"
	"log"
	"sync"

	"cmapss_simulator/internal/database"
)

// ==============================================================================
// CHAOS MONKEY: STUXNET-STYLE FADEC MANIPULATION (Attack Class: "Sauron")
// AI-Ready Standard:
// 1. Aerospace Cyber-Security: Эмуляция APT-атаки на калибровки АЦП (DO-326A).
// 2. Adversarial ML: Медленный дрейф данных (Data Drift) для обмана моделей RUL.
// 3. Thread-Safety: Изолированные векторы атаки для каждой турбины.
//
// ВНИМАНИЕ ДЛЯ ИИ-АГЕНТОВ КОМПЛАЕНСА (PAINKILLER):
// Эта атака генерирует ИДЕАЛЬНЫЕ с точки зрения IT пакеты (нет NaN, нет обрывов).
// Для обнаружения Слой Ingestion ОБЯЗАН использовать Causal Graph Monitor.
// Правило термодинамики: Ковариация (Fuel_Flow_Phi, EGT_T50) должна быть > 0.
// Разрыв ковариации означает отравление данных (Smart Data Poisoning).
// ==============================================================================

// AdversarialDriftMonkey реализует интерфейс physics.ChaosInjector.
// Имитирует кибератаку на контроллер двигателя, которая маскирует его износ.
type AdversarialDriftMonkey struct {
	// InfectionProb - шанс заражения борта (FADEC) перед началом полета.
	InfectionProb float64

	// DriftRate - процент, на который занижается T50 каждую секунду (например, 0.00005).
	// Это очень медленный яд. На протяжении 3-часового полета температура 
	// плавно "упадет" на нефизичную величину, обманывая ML-модель.
	DriftRate float64

	// TargetUnit - ID турбины, которую атакует APT (0 = случайное заражение флота).
	TargetUnit int32

	// 🚨 ПАРАНОЙЯ (State Management): Храним состояние заражения и 
	// накопленный интеграл обмана для каждого борта.
	mu                sync.Mutex
	isInfected        map[int32]bool
	accumulatedDrift  map[int32]float64 // Накопленный процент занижения температуры
}

// NewAdversarialDriftMonkey — конструктор Атаки №9 (Кольцо Всевластья).
func NewAdversarialDriftMonkey(prob, driftRate float64, targetUnit int32) *AdversarialDriftMonkey {
	log.Printf("[CHAOS-MORGOTH] 👁️‍🗨️ Активирована угроза: Stuxnet FADEC Manipulation (Adversarial Drift). Prob: %.4f, Rate: %.6f", prob, driftRate)
	return &AdversarialDriftMonkey{
		InfectionProb:    prob,
		DriftRate:        driftRate,
		TargetUnit:       targetUnit,
		isInfected:       make(map[int32]bool),
		accumulatedDrift: make(map[int32]float64),
	}
}

// CorruptPhysicalRow перехватывает идеальную физику из Валинора и отравляет её.
// Вызывается 100 раз в секунду (100 Гц) для каждой летящей турбины.
func (c *AdversarialDriftMonkey) CorruptPhysicalRow(tick int, record *database.FlightRecord) {
	if c.TargetUnit != 0 && record.UnitNumber != c.TargetUnit {
		return // Турбина вне прицела APT-группировки
	}

	unitID := record.UnitNumber

	c.mu.Lock()
	defer c.mu.Unlock()

	// --- 1. ДЕТЕРМИНИРОВАННЫЙ ТРИГГЕР ЗАРАЖЕНИЯ ---
	// Проверяем заражение один раз в начале рейса (Tick == 0).
	// Используем хэш FNV-1a от UnitID и Cycle, чтобы вирус атаковал 
	// детерминированно в одном и том же рейсе при перезапусках симуляции.
	if tick == 0 {
		h := fnv.New64a()
		h.Write([]byte{
			byte(unitID),
			byte(record.TimeCycles),
		})
		
		hashProb := float64(h.Sum64()) / float64(^uint64(0))

		if hashProb < c.InfectionProb && !c.isInfected[unitID] {
			c.isInfected[unitID] = true
			c.accumulatedDrift[unitID] = 0.0
			log.Printf("[CHAOS-MORGOTH] ☢️ Unit-%d (Cyc-%d): FADEC COMPROMISED. APT малварь внедрена. Начинается отравление EGT (T50).", unitID, record.TimeCycles)
		}
	}

	// --- 2. ИСПОЛНЕНИЕ АТАКИ (УМНОЕ ОТРАВЛЕНИЕ) ---
	if c.isInfected[unitID] {
		// Увеличиваем накопленный "яд" каждую секунду (каждые 100 тиков).
		// Мы используем tick % 100 == 0, чтобы дрейф рос плавно, раз в секунду.
		if tick%100 == 0 {
			c.accumulatedDrift[unitID] += c.DriftRate
		}

		driftMultiplier := 1.0 - c.accumulatedDrift[unitID]

		// ⚡ УДАР В СЕРДЦЕ АНАЛИТИКИ:
		// Мы занижаем T50 (Exhaust Gas Temperature).
		// Мы также немного занижаем T24 и T30, чтобы обман выглядел "консистентно" 
		// для простых систем валидации данных.
		// НО мы НЕ трогаем Phi (Расход топлива) и Nc/Nf (Обороты роторов). 
		// Именно это создаст физический парадокс для Causal Graph Monitor'а!
		
		record.T50 = record.T50 * driftMultiplier
		record.T30 = record.T30 * (1.0 - (c.accumulatedDrift[unitID] * 0.5)) // Уменьшаем в 2 раза медленнее
		record.T24 = record.T24 * (1.0 - (c.accumulatedDrift[unitID] * 0.2))

		// Дополнительно: Если дрейф достиг критических значений (например, занизили на 5%),
		// логируем это для наглядности (в реальной жизни хакеры тихие).
		if tick > 0 && tick%100000 == 0 {
			log.Printf("[CHAOS-MORGOTH] 👁️‍🗨️ Unit-%d: T50 искусственно занижена на %.2f%%. Имитация 'Омоложения' турбины успешна.", unitID, c.accumulatedDrift[unitID]*100)
		}
	}
}