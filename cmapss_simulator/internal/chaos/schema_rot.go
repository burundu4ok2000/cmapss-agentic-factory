package chaos

import (
	"hash/fnv"
	"log"
	"math"

	"cmapss_simulator/internal/database"
)

// ==============================================================================
// CHAOS MONKEY: ARINC 429 RADIATION BIT-FLIP (Attack Class: "Orclings")
// AI-Ready Standard:
// 1. Aerospace Compliance (DO-178C / DO-326A).
// 2. 100% Deterministic execution (Reproducible in CI/CD).
// 3. Simulates physical Single Event Upset (SEU) from cosmic radiation.
// ==============================================================================

// Arinc429BitFlipInjector реализует интерфейс physics.ChaosInjector.
// Симулирует распад структуры данных при передаче по авиационной шине.
type Arinc429BitFlipInjector struct {
	// Probability - вероятность удара космической частицы на один полетный тик.
	// В реальности (на эшелоне) это около 10^-9 на бит в час, но для стресс-теста
	// Завода мы повышаем вероятность (например: 0.00005).
	Probability float64

	// TargetUnit - ID турбины, которую атакует Моргот (0 - атакуем случайные).
	TargetUnit int32
}

// NewArinc429BitFlipInjector — конструктор атаки №1.
func NewArinc429BitFlipInjector(prob float64, targetUnit int32) *Arinc429BitFlipInjector {
	log.Printf("[CHAOS-MORGOTH] ☢️ Активирована угроза ARINC 429 Bit-Flip. Вероятность: %f", prob)
	return &Arinc429BitFlipInjector{
		Probability: prob,
		TargetUnit:  targetUnit,
	}
}

// CorruptPhysicalRow — точка инъекции хаоса в идеальный мир Валинора.
// Вызывается перед записью строки в C-память Arrow.
func (c *Arinc429BitFlipInjector) CorruptPhysicalRow(tick int, record *database.FlightRecord) {
	// Если атака нацелена на конкретный борт, а летит другой — пропускаем.
	if c.TargetUnit != 0 && record.UnitNumber != c.TargetUnit {
		return
	}

	// 🚨 ПАРАНОЙЯ (100% Determinism):
	// Мы НЕ используем math/rand(). В авиационном софте (DO-178C) тесты обязаны
	// быть воспроизводимыми. Мы генерируем радиационный удар на основе криптографического
	// хэша от неизменных параметров (Unit, Cycle, Tick).
	// Это гарантирует, что при перезапуске симуляции нейтрон ударит в ту же самую миллисекунду.
	h := fnv.New64a()
	h.Write([]byte{byte(record.UnitNumber), byte(record.TimeCycles), byte(tick >> 8), byte(tick)})
	hashVal := h.Sum64()

	// Преобразуем хэш [0...MaxUint64] в вероятность float64 [0.0...1.0]
	hashProb := float64(hashVal) / float64(^uint64(0))

	// Если вероятность совпала — происходит Single Event Upset (SEU)
	if hashProb < c.Probability {
		// ==================================================================
		// 💥 УДАР РАДИАЦИИ В 32-БИТНОЕ СЛОВО ARINC 429
		// ==================================================================
		
		// Решаем, в какой именно сектор 32-битного слова попала частица:
		hitZone := hashVal % 3

		switch hitZone {
		case 0:
			// СЦЕНАРИЙ А: Мутация Label (Биты 1-8) или SDI (Биты 9-10).
			// Система теряет идентификатор источника. Пакет становится "сиротой".
			// Когда этот пакет долетит до Spark, downstream-система (Painkiller)
			// обязана поймать нарушение Foreign Key контракта (unit_number <= 0),
			// изолировать эти байты и отправить в DLQ (Dead Letter Queue).
			log.Printf("[CHAOS-EVENT] ⚠️ Unit-%d (Cyc-%d, Tick-%d): ARINC Label/SDI Corruption! Loss of Identity.", 
				record.UnitNumber, record.TimeCycles, tick)
			
			record.UnitNumber = -9999 // Имитация разрушенного идентификатора

		case 1:
			// СЦЕНАРИЙ Б: Мутация Sign/Status Matrix (SSM) (Биты 30-31).
			// Бит переключился из 11 (Normal BNR) в 01 (No Computed Data).
			// Микроконтроллер FADEC не смог распарсить данные и выдал "Не число".
			// Мы эмулируем это через IEEE 754 NaN. Parquet запишет это как Null/NaN.
			// Painkiller (Data Quality Gate) обязан поймать NaN и пометить партицию тегом DQ_FAILED.
			log.Printf("[CHAOS-EVENT] ⚠️ Unit-%d (Cyc-%d, Tick-%d): ARINC SSM Bit-Flip! htBleed -> NaN.", 
				record.UnitNumber, record.TimeCycles, tick)
			
			record.Htbleed = math.NaN()

		case 2:
			// СЦЕНАРИЙ В: Мутация Payload (Биты 11-29) - Экспонента.
			// Радиация перевернула бит экспоненты (Exponent Bit-Flip) числа с плавающей точкой.
			// Значение улетело в бесконечность, хотя двигатель работает абсолютно штатно.
			// Это классический Data Drift, который ломает алгоритмы машинного обучения.
			log.Printf("[CHAOS-EVENT] ⚠️ Unit-%d (Cyc-%d, Tick-%d): Payload Exponent Bit-Flip! htBleed -> +Inf.", 
				record.UnitNumber, record.TimeCycles, tick)
			
			record.Htbleed = math.Inf(1)
		}
	}
}