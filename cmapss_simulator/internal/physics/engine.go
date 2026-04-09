package physics

import (
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"time"

	"cmapss_simulator/internal/database"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// applyNoise накладывает Гауссовский (белый) шум амплитудой 0.01% на базовое значение.
// AI-Ready: Использует локальный rng для предотвращения блокировок (Global Lock).
func applyNoise(baseValue float64, rng *rand.Rand) float64 {
	if baseValue == 0 {
		return 0
	}
	// rng.NormFloat64() дает нормальное распределение (mean=0, stddev=1).
	// Умножаем на базовое значение и коэффициент 0.0001 (0.01%).
	return baseValue + (rng.NormFloat64() * baseValue * 0.0001)
}

// GenerateTelemetry наполняет предоставленный Arrow Builder зашумленными данными.
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

	// 🚨 ПАРАНОЙЯ L3 (Zero-Allocation Loop Pattern):
	// Чтобы не тратить CPU на приведение типов (Type Assertion) внутри цикла из 30 000 итераций,
	// мы получаем строго типизированные указатели на колонки ДО начала цикла.
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

	// Основной физический цикл (Game Loop)
	for i := 0; i < totalTicks; i++ {
		// Время строго математическое, никаких time.Now()
		tickTime := startTime.Add(time.Duration(i) * tickDuration)

		// 1. Системные данные
		tsB.Append(arrow.Timestamp(tickTime.UnixMilli()))
		unitB.Append(baseRec.UnitNumber)
		cycleB.Append(baseRec.TimeCycles)

		// 2. Настройки (Обычно статичны, но можем добавить микрошум для реализма)
		op1B.Append(applyNoise(baseRec.OpSetting1, rng))
		op2B.Append(applyNoise(baseRec.OpSetting2, rng))
		op3B.Append(baseRec.OpSetting3) // Дроссель обычно фиксирован ступенчато

		// 3. Зашумленные датчики
		t2B.Append(applyNoise(baseRec.T2, rng))
		t24B.Append(applyNoise(baseRec.T24, rng))
		t30B.Append(applyNoise(baseRec.T30, rng))
		t50B.Append(applyNoise(baseRec.T50, rng))
		p2B.Append(applyNoise(baseRec.P2, rng))
		p15B.Append(applyNoise(baseRec.P15, rng))
		p30B.Append(applyNoise(baseRec.P30, rng))
		nfB.Append(applyNoise(baseRec.Nf, rng))
		ncB.Append(applyNoise(baseRec.Nc, rng))
		eprB.Append(applyNoise(baseRec.Epr, rng))
		ps30B.Append(applyNoise(baseRec.Ps30, rng))
		phiB.Append(applyNoise(baseRec.Phi, rng))
		nrfB.Append(applyNoise(baseRec.NRf, rng))
		nrcB.Append(applyNoise(baseRec.NRc, rng))
		bprB.Append(applyNoise(baseRec.BPR, rng))
		farBB.Append(applyNoise(baseRec.FarB, rng))
		htBleedB.Append(applyNoise(baseRec.HtBleed, rng))
		nfdmdB.Append(applyNoise(baseRec.Nf_dmd, rng))
		pcnfrdmdB.Append(applyNoise(baseRec.PCNfR_dmd, rng))
		w31B.Append(applyNoise(baseRec.W31, rng))
		w32B.Append(applyNoise(baseRec.W32, rng))
	}

	return nil
}
