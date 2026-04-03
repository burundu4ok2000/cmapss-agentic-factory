package chaos

import (
	"hash/fnv"
	"log"
	"sync"

	"cmapss_simulator/internal/database"
)

// ==============================================================================
// CHAOS MONKEY: SENSOR FREEZE / PITOT ICING (Attack Class: "Balrogs")
// AI-Ready Standard:
// 1. Aerospace Physics: Эмуляция закупорки трубки Пито (Ice/Carbon Blockage).
// 2. OT-Blindness: Датчик зависает на константе. Дисперсия падает до нуля.
// 3. Thread-Safety: Использование RWMutex для изоляции состояния конкретных турбин.
//
// ВНИМАНИЕ ДЛЯ ИИ-АГЕНТОВ КОМПЛАЕНСА (PAINKILLER):
// Этот слой генерирует "Плоскую линию" для датчика P30.
// Слой Spark Streaming ОБЯЗАН внедрить алгоритм Аналитической Избыточности.
// Правило: Если Var(P30) в окне 30 сек == 0 И stddev(Nc) > 0 -> Отказ P30.
// Пайплайн должен активировать Виртуальный Датчик (Virtual Sensor Imputation).
// ==============================================================================

// SensorFreezeMonkey реализует интерфейс physics.ChaosInjector.
// Имитирует замерзание или закоксовывание датчика давления P30.
type SensorFreezeMonkey struct {
	// Probability - шанс обледенения на каждом физическом тике.
	Probability float64

	// TargetUnit - ID турбины (0 = атакуем весь флот).
	TargetUnit int32

	// 🚨 ПАРАНОЙЯ (State Management):
	// Обмерзание - это длительный процесс. Если датчик "замерз", он будет 
	// выдавать одно и то же значение, пока не "оттает" (в нашей симуляции - до конца полета).
	// Мы должны запомнить то самое значение, на котором он завис.
	mu            sync.RWMutex
	frozenValue   map[int32]float64 // K: UnitNumber, V: Значение, на котором завис датчик
	isSensorIced  map[int32]bool
}

// NewSensorFreezeMonkey — конструктор Атаки №6.
func NewSensorFreezeMonkey(prob float64, targetUnit int32) *SensorFreezeMonkey {
	log.Printf("[CHAOS-MORGOTH] 🧊 Активирована угроза: Sensor Freeze (P30 Blockage). Вероятность: %f", prob)
	return &SensorFreezeMonkey{
		Probability:  prob,
		TargetUnit:   targetUnit,
		frozenValue:  make(map[int32]float64),
		isSensorIced: make(map[int32]bool),
	}
}

// CorruptPhysicalRow перехватывает физику и "замораживает" микрошум.
// Вызывается 100 раз в секунду (100 Гц) для каждой летящей турбины.
func (c *SensorFreezeMonkey) CorruptPhysicalRow(tick int, record *database.FlightRecord) {
	if c.TargetUnit != 0 && record.UnitNumber != c.TargetUnit {
		return // Турбина вне зоны поражения
	}

	unitID := record.UnitNumber

	// --- 1. ПРОВЕРКА ФИЗИЧЕСКОГО СОСТОЯНИЯ (Уже замерз?) ---
	c.mu.RLock()
	iced := c.isSensorIced[unitID]
	frozenVal := c.frozenValue[unitID]
	c.mu.RUnlock()

	if iced {
		// Трубка Пито забита. Датчик аппаратно выдает то давление, 
		// которое осталось запертым внутри трубки в момент обледенения.
		record.P30 = frozenVal
		return
	}

	// --- 2. ДЕТЕРМИНИРОВАННЫЙ ТРИГГЕР ОБЛЕДЕНЕНИЯ ---
	// DO-178C (Воспроизводимость). Хэш от неизменных параметров.
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
		c.isSensorIced[unitID] = true
		// Запоминаем идеальное физическое значение давления в эту миллисекунду
		c.frozenValue[unitID] = record.P30 
		c.mu.Unlock()

		log.Printf("[CHAOS-MORGOTH] ❄️ Unit-%d (Cyc-%d, Tick-%d): Pitot Blockage! Датчик P30 замерз на значении %.2f psia.", 
			unitID, record.TimeCycles, tick, record.P30)
	}
}