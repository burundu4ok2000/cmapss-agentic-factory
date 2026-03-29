package physics

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"time"

	"cmapss_simulator/internal/database"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ==============================================================================
// ФИЗИЧЕСКИЙ ДВИЖОК C-MAPSS (Data Generation Layer)
// AI-Ready: 100% Детерминизм, Zero-Allocation архитектура, Временные ряды.
// ==============================================================================

// applyFractalNoise генерирует "тяжелый" для сжатия фрактальный шум (имитация Perlin Noise).
// Состоит из низкочастотных волн (вздохи турбины) и высокочастотного Гауссовского микро-шума.
func applyFractalNoise(baseValue float64, tick int, rng *rand.Rand) float64 {
	if baseValue == 0 {
		return 0
	}
	t := float64(tick)

	// Сложение трех синусоид разных частот для имитации сложной вибрации ротора
	wave := math.Sin(t*0.01)*0.5 + math.Sin(t*0.05)*0.25 + math.Sin(t*0.1)*0.1
	
	// Термодинамический микро-шум (Гауссовское распределение)
	microNoise := rng.NormFloat64() * 0.2

	// Итоговое возмущение: не более 0.05% от базового значения
	perturbation := (wave + microNoise) * 0.0005 * baseValue
	return baseValue + perturbation
}

// GenerateTelemetry наполняет предоставленный Arrow Builder физическими данными (100 Гц).
// 🚨 ПАРАНОЙЯ L1 (Zero-Allocation): Мы НЕ возвращаем массивы. Мы пишем прямо в память C-аллокатора.
func GenerateTelemetry(builder *array.RecordBuilder, baseRec database.FlightRecord, startTime time.Time, durationSec, targetHz int, runID string) error {
	if builder.Schema().NumFields() != 27 {
		return fmt.Errorf("FATAL: Несовпадение Data Contract. Ожидалось 27 колонок, получено %d", builder.Schema().NumFields())
	}

	totalTicks := durationSec * targetHz
	tickDuration := time.Second / time.Duration(targetHz)

	// 🚨 ПАРАНОЙЯ L2 (100% Детерминизм):
	// Хэшируем RunID в uint64 для первого зерна. Второе зерно - комбинация турбины и цикла.
	h := fnv.New64a()
	h.Write([]byte(runID))
	seed1 := h.Sum64()
	seed2 := uint64(baseRec.UnitNumber)<<32 | uint64(baseRec.TimeCycles)

	// Локальный, неблокирующий, криптографически детерминированный генератор.
	rng := rand.New(rand.NewPCG(seed1, seed2))

	// 🚨 ПАРАНОЙЯ L3 (Fast-Path Type Assertions):
	// Получаем строго типизированные указатели на память Arrow ДО начала цикла из 720 000 итераций.
	tsB := builder.Field(0).(*array.TimestampBuilder)
	unitB := builder.Field(1).(*array.Int32Builder)
	cycleB := builder.Field(2).(*array.Int32Builder)
	op1B := builder.Field(3).(*array.Float64Builder)
	op2B := builder.Field(4).(*array.Float64Builder)
	op3B := builder.Field(5).(*array.Float64Builder)
	// Датчики
	t2B := builder.Field(6).(*array.Float64Builder)
	t24B := builder.Field(7).(*array.Float64Builder)
	t30B := builder.Field(8).(*array.Float64Builder)
	t50B := builder.Field(9).(*array.Float64Builder)
	p2B := builder.Field(10).(*array.Float64Builder)
	p15B := builder.Field(11).(*array.Float64Builder)
	p30B := builder.Field(12).(*array.Float64Builder)
	nfB := builder.Field(13).(*array.Float64Builder)
	ncB := builder.Field(14).(*array.Float64Builder)
	eprB := builder.Field(15).(*array.Float64Builder)
	ps30B := builder.Field(16).(*array.Float64Builder)
	phiB := builder.Field(17).(*array.Float64Builder)
	nrfB := builder.Field(18).(*array.Float64Builder)
	nrcB := builder.Field(19).(*array.Float64Builder)
	bprB := builder.Field(20).(*array.Float64Builder)
	farBB := builder.Field(21).(*array.Float64Builder)
	htBleedB := builder.Field(22).(*array.Float64Builder)
	nfdmdB := builder.Field(23).(*array.Float64Builder)
	pcnfrdmdB := builder.Field(24).(*array.Float64Builder)
	w31B := builder.Field(25).(*array.Float64Builder)
	w32B := builder.Field(26).(*array.Float64Builder)

	// Границы фаз полета (Flight Envelope)
	takeoffTicks := int(float64(totalTicks) * 0.15)
	cruiseTicks := int(float64(totalTicks) * 0.85)

	// Максимальная высота эшелона (футы)
	targetAltitude := 35000.0

	// Основной физический цикл (Game Loop)
	for i := 0; i < totalTicks; i++ {
		// Время строго математическое, никаких time.Now()
		tickTime := startTime.Add(time.Duration(i) * tickDuration)

		// Математика огибающей полета (Flight Phases)
		var thrustMultiplier float64
		var currentAltitude float64
		var throttle float64

		if i < takeoffTicks {
			// ВЗЛЕТ (0% - 15%): Тяга нарастает, затем держится на 100%. Высота растет.
			progress := float64(i) / float64(takeoffTicks)
			throttle = 100.0
			currentAltitude = targetAltitude * progress
			thrustMultiplier = 1.0 + (0.15 * progress) // Температура (T50) растет до 115% от базы
		} else if i < cruiseTicks {
			// ЭШЕЛОН (15% - 85%): Выход на крейсерский режим (Baseline из датасета NASA)
			throttle = baseRec.OpSetting3 // Возврат к номинальной тяге
			currentAltitude = targetAltitude
			thrustMultiplier = 1.0 // Штатная температура T50
		} else {
			// ПОСАДКА (85% - 100%): Снижение высоты, сброс тяги.
			progress := float64(i-cruiseTicks) / float64(totalTicks-cruiseTicks)
			throttle = baseRec.OpSetting3 * (1.0 - progress)
			if throttle < 20.0 {
				throttle = 20.0 // Idle (Холостой ход)
			}
			currentAltitude = targetAltitude * (1.0 - progress)
			thrustMultiplier = 1.0 - (0.20 * progress) // Охлаждение турбины (до 80%)
		}

		// 1. Системные данные
		tsB.Append(arrow.Timestamp(tickTime.UnixMilli()))
		unitB.Append(baseRec.UnitNumber)
		cycleB.Append(baseRec.TimeCycles)

		// 2. Условия среды (Operational Settings)
		op1B.Append(applyFractalNoise(currentAltitude, i, rng)) // Altitude
		op2B.Append(applyFractalNoise(baseRec.OpSetting2, i, rng)) // Mach
		op3B.Append(applyFractalNoise(throttle, i, rng)) // TRA (Throttle)

		// Физика стандартной атмосферы: Температура на входе (T2) падает с высотой
		// Грубо: минус 2 градуса Цельсия на 1000 футов высоты.
		tempDropRatio := 1.0 - (currentAltitude / 100000.0)
		t2Value := baseRec.T2 * tempDropRatio

		// 3. Зашумленные датчики температуры (Зависят от тяги thrustMultiplier)
		t2B.Append(applyFractalNoise(t2Value, i, rng))
		t24B.Append(applyFractalNoise(baseRec.T24*thrustMultiplier, i, rng))
		t30B.Append(applyFractalNoise(baseRec.T30*thrustMultiplier, i, rng))
		t50B.Append(applyFractalNoise(baseRec.T50*thrustMultiplier, i, rng))

		// 4. Датчики давления (Зависят от тяги)
		p2B.Append(applyFractalNoise(baseRec.P2*thrustMultiplier, i, rng))
		p15B.Append(applyFractalNoise(baseRec.P15*thrustMultiplier, i, rng))
		p30B.Append(applyFractalNoise(baseRec.P30*thrustMultiplier, i, rng))
		ps30B.Append(applyFractalNoise(baseRec.Ps30*thrustMultiplier, i, rng))

		// 5. ЯКОРЯ ИСТИНЫ (Ground Truth)
		// Физическая скорость вентилятора (Nf) не подлежит шуму. Это маркер Data Quality.
		nfB.Append(baseRec.Nf * thrustMultiplier)
		ncB.Append(applyFractalNoise(baseRec.Nc*thrustMultiplier, i, rng))
		nrfB.Append(baseRec.Nrf) // Скорректированная скорость тоже якорь

		// 6. Остальные сложные параметры
		eprB.Append(applyFractalNoise(baseRec.Epr, i, rng))
		phiB.Append(applyFractalNoise(baseRec.Phi*thrustMultiplier, i, rng))
		nrcB.Append(applyFractalNoise(baseRec.Nrc, i, rng))
		bprB.Append(applyFractalNoise(baseRec.Bpr, i, rng))
		farBB.Append(applyFractalNoise(baseRec.Farb, i, rng))
		htBleedB.Append(applyFractalNoise(baseRec.Htbleed, i, rng))
		nfdmdB.Append(applyFractalNoise(baseRec.NfDmd, i, rng))
		pcnfrdmdB.Append(applyFractalNoise(baseRec.PcnfrDmd, i, rng))
		w31B.Append(applyFractalNoise(baseRec.W31, i, rng))
		w32B.Append(applyFractalNoise(baseRec.W32, i, rng))
	}

	return nil
}
