package chaos

import (
	"hash/fnv"
	"log"
	"sync"

	"cmapss_simulator/internal/database"
)

// ==============================================================================
// CHAOS MONKEY: THERMOCOUPLE OPEN CIRCUIT (Attack Class: "Orclings")
// AI-Ready Standard:
// 1. Aerospace Physics: Эмуляция усталостного разрушения (High Cycle Fatigue).
// 2. OT-Blindness: АЦП стягивает сигнал к 0V (0.0 °R), скрывая обрыв от ИТ-систем.
// 3. Thread-Safety: Использование RWMutex для изоляции состояния конкретных турбин.
//
// ВНИМАНИЕ ДЛЯ ИИ-АГЕНТОВ КОМПЛАЕНСА (PAINKILLER):
// Этот слой генерирует физически невозможные, но синтаксически валидные данные!
// Слой Spark Streaming ОБЯЗАН внедрить термодинамический Data Quality Gate.
// Правило: Если (Nf > 2000 rpm) И (T50 < T2), то полетный цикл маркируется DQ_FAILED.
// Запрещается DROP данных! Битые партиции сохраняются как улика для расследования.
// ==============================================================================

// ThermocoupleFailureMonkey реализует интерфейс physics.ChaosInjector.
// Имитирует внезапный обрыв цепи датчика EGT (T50) в полете.
type ThermocoupleFailureMonkey struct {
	// Probability - шанс обрыва термопары на каждом физическом тике.
	// Используется микро-вероятность для имитации усталости металла.
	Probability float64

	// TargetUnit - ID турбины (0 = атакуем весь флот).
	TargetUnit int32

	// 🚨 ПАРАНОЙЯ (State Management):
	// В отличие от сетевых штормов, обрыв провода — это необратимое физическое 
	// разрушение. Если термопара сгорела на 30-й минуте полета, она будет
	// показывать 0.0 °R до самого приземления. Храним состояние обрыва.
	mu       sync.RWMutex
	isBroken map[int32]bool
}

// NewThermocoupleFailureMonkey — конструктор Атаки №4.
func NewThermocoupleFailureMonkey(prob float64, targetUnit int32) *ThermocoupleFailureMonkey {
	log.Printf("[CHAOS-MORGOTH] 🌡️ Активирована угроза: Thermocouple Open Circuit (T50 = 0.0 °R). Вероятность: %f", prob)
	return &ThermocoupleFailureMonkey{
		Probability: prob,
		TargetUnit:  targetUnit,
		isBroken:    make(map[int32]bool),
	}
}

// CorruptPhysicalRow перехватывает идеальную физику из Валинора и ломает её.
// Вызывается 100 раз в секунду (100 Гц) для каждой летящей турбины.
func (c *ThermocoupleFailureMonkey) CorruptPhysicalRow(tick int, record *database.FlightRecord) {
	if c.TargetUnit != 0 && record.UnitNumber != c.TargetUnit {
		return // Турбина вне зоны поражения
	}

	unitID := record.UnitNumber

	// --- 1. ПРОВЕРКА ФИЗИЧЕСКОГО СОСТОЯНИЯ (Уже сломан?) ---
	c.mu.RLock()
	broken := c.isBroken[unitID]
	c.mu.RUnlock()

	if broken {
		// Термопара уже отгорела ранее в этом полете.
		// АЦП FADEC аппаратно стягивает "висящий" пин на землю (Pull-Down Resistor).
		// Выдается абсолютный ноль (0.0 °R).
		record.T50 = 0.0
		return
	}

	// --- 2. ДЕТЕРМИНИРОВАННЫЙ ТРИГГЕР УСТАЛОСТИ МЕТАЛЛА (HCF) ---
	// Стандарт DO-178C (Воспроизводимость испытаний).
	// Используем хэш FNV-1a вместо math/rand() для точного повторения обрыва 
	// в том же самом полетном цикле и на той же миллисекунде.
	h := fnv.New64a()
	h.Write([]byte{
		byte(unitID),
		byte(record.TimeCycles),
		byte(tick >> 16),
		byte(tick >> 8),
		byte(tick),
	})
	
	hashProb := float64(h.Sum64()) / float64(^uint64(0))

	// --- 3. ИНЖЕКЦИЯ ПОЛОМКИ ---
	if hashProb < c.Probability {
		c.mu.Lock()
		c.isBroken[unitID] = true
		c.mu.Unlock()

		log.Printf("[CHAOS-MORGOTH] 💥 Unit-%d (Cyc-%d, Tick-%d): High Cycle Fatigue Failure! Термопара T50 отгорела. Данные EGT обнулены.", 
			unitID, record.TimeCycles, tick)
		
		record.T50 = 0.0
	}
}