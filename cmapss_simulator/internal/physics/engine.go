package physics

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"time"

	"cmapss_simulator/internal/database"
	"cmapss_simulator/internal/domain" // Внедрение Подвала (Dependency Inversion)

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ==============================================================================
// ФИЗИЧЕСКИЙ ДВИЖОК C-MAPSS (Data Generation Layer - Валинор)
// AI-Ready Standard:
// 1. 100% Детерминизм (Zero-Allocation).
// 2. Внедрен интерфейс ChaosInjector из Подвала (Инженерия Хаоса без циклов).
// 3. Ground Truth: Датчик Nf (Скорость вентилятора) защищен от искажений.
// ==============================================================================

// applyFractalNoise генерирует "тяжелый" для сжатия фрактальный шум (Pseudo-Perlin).
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

// GenerateTelemetry наполняет Arrow Builder физическими данными (100 Гц).
// 🚨 ПАРАНОЙЯ L1 (Zero-Allocation): Мы пишем прямо в память C-аллокатора.
// Внедрена точка инъекции хаоса (chaosInjector типа domain.ChaosInjector).
func GenerateTelemetry(builder *array.RecordBuilder, baseRec database.FlightRecord, startTime time.Time, durationSec, targetHz int, runID string, chaosInjector domain.ChaosInjector) error {
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

	// Инициализируем структуру для мутаций текущего тика
	currentRow := baseRec

	// Основной физический цикл (Game Loop)
	for i := 0; i < totalTicks; i++ {
		// Время строго математическое, никаких time.Now()
		tickTime := startTime.Add(time.Duration(i) * tickDuration)

		// --- 1. ВЫЧИСЛЕНИЕ ИДЕАЛЬНОЙ ФИЗИКИ (Валинор) ---
		var thrustMultiplier float64
		var currentAltitude float64
		var throttle float64

		if i < takeoffTicks {
			// ВЗЛЕТ (0% - 15%): Тяга нарастает, затем держится на 100%. Высота растет.
			progress := float64(i) / float64(takeoffTicks)
			throttle = 100.0
			currentAltitude = targetAltitude * progress
			thrustMultiplier = 1.0 + (0.15 * progress)
		} else if i < cruiseTicks {
			throttle = baseRec.OpSetting3
			currentAltitude = targetAltitude
			thrustMultiplier = 1.0
		} else {
			// ПОСАДКА (85% - 100%): Снижение высоты, сброс тяги.
			progress := float64(i-cruiseTicks) / float64(totalTicks-cruiseTicks)
			throttle = baseRec.OpSetting3 * (1.0 - progress)
			if throttle < 20.0 { throttle = 20.0 }
			currentAltitude = targetAltitude * (1.0 - progress)
			thrustMultiplier = 1.0 - (0.20 * progress)
		}

		tempDropRatio := 1.0 - (currentAltitude / 100000.0)
		t2Value := baseRec.T2 * tempDropRatio

		// Наполняем структуру идеальными, зашумленными (естественным путем) данными
		currentRow.OpSetting1 = applyFractalNoise(currentAltitude, i, rng)
		currentRow.OpSetting2 = applyFractalNoise(baseRec.OpSetting2, i, rng)
		currentRow.OpSetting3 = applyFractalNoise(throttle, i, rng)

		currentRow.T2 = applyFractalNoise(t2Value, i, rng)
		currentRow.T24 = applyFractalNoise(baseRec.T24*thrustMultiplier, i, rng)
		currentRow.T30 = applyFractalNoise(baseRec.T30*thrustMultiplier, i, rng)
		currentRow.T50 = applyFractalNoise(baseRec.T50*thrustMultiplier, i, rng)

		currentRow.P2 = applyFractalNoise(baseRec.P2*thrustMultiplier, i, rng)
		currentRow.P15 = applyFractalNoise(baseRec.P15*thrustMultiplier, i, rng)
		currentRow.P30 = applyFractalNoise(baseRec.P30*thrustMultiplier, i, rng)
		currentRow.Ps30 = applyFractalNoise(baseRec.Ps30*thrustMultiplier, i, rng)

		// GROUND TRUTH: Физическая скорость вентилятора (Nf) не подлежит шуму
		currentRow.Nf = baseRec.Nf * thrustMultiplier
		
		currentRow.Nc = applyFractalNoise(baseRec.Nc*thrustMultiplier, i, rng)
		currentRow.Epr = applyFractalNoise(baseRec.Epr, i, rng)
		currentRow.Phi = applyFractalNoise(baseRec.Phi*thrustMultiplier, i, rng)
		currentRow.Nrf = baseRec.Nrf // Скорректированная скорость тоже якорь
		currentRow.Nrc = applyFractalNoise(baseRec.Nrc, i, rng)
		currentRow.Bpr = applyFractalNoise(baseRec.Bpr, i, rng)
		currentRow.Farb = applyFractalNoise(baseRec.Farb, i, rng)
		currentRow.Htbleed = applyFractalNoise(baseRec.Htbleed, i, rng)
		currentRow.NfDmd = applyFractalNoise(baseRec.NfDmd, i, rng)
		currentRow.PcnfrDmd = applyFractalNoise(baseRec.PcnfrDmd, i, rng)
		currentRow.W31 = applyFractalNoise(baseRec.W31, i, rng)
		currentRow.W32 = applyFractalNoise(baseRec.W32, i, rng)

		// --- 2. ВТОРЖЕНИЕ ХАОСА (Моргот) ---
		// Если активирован Chaos Monkey, он может незаметно изменить поля currentRow
		// ДО записи их в Arrow-буфер.
		if chaosInjector != nil {
			chaosInjector.CorruptPhysicalRow(i, &currentRow)
		}

		// --- 3. ЗАПИСЬ В C-ПАМЯТЬ (Arrow) ---
		tsB.Append(arrow.Timestamp(tickTime.UnixMilli()))
		unitB.Append(baseRec.UnitNumber)
		cycleB.Append(baseRec.TimeCycles)

		op1B.Append(currentRow.OpSetting1)
		op2B.Append(currentRow.OpSetting2)
		op3B.Append(currentRow.OpSetting3)
		
		t2B.Append(currentRow.T2)
		t24B.Append(currentRow.T24)
		t30B.Append(currentRow.T30)
		t50B.Append(currentRow.T50)
		
		p2B.Append(currentRow.P2)
		p15B.Append(currentRow.P15)
		p30B.Append(currentRow.P30)
		
		nfB.Append(currentRow.Nf)
		ncB.Append(currentRow.Nc)
		eprB.Append(currentRow.Epr)
		ps30B.Append(currentRow.Ps30)
		phiB.Append(currentRow.Phi)
		nrfB.Append(currentRow.Nrf)
		nrcB.Append(currentRow.Nrc)
		bprB.Append(currentRow.Bpr)
		farBB.Append(currentRow.Farb)
		htBleedB.Append(currentRow.Htbleed)
		nfdmdB.Append(currentRow.NfDmd)
		pcnfrdmdB.Append(currentRow.PcnfrDmd)
		w31B.Append(currentRow.W31)
		w32B.Append(currentRow.W32)
	}

	return nil
}
