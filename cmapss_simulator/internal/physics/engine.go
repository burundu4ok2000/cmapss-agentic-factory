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
func GenerateTelemetry(builder *array.RecordBuilder, baseRec database.FlightRecord, startTime time.Time, durationSec, targetHz int, runID string, transmit func(interface{})) error {
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
		op1 := applyNoise(baseRec.OpSetting1, rng)
		op1B.Append(op1)
		op2 := applyNoise(baseRec.OpSetting2, rng)
		op2B.Append(op2)
		op3B.Append(baseRec.OpSetting3)

		// 3. Зашумленные датчики
		t2 := applyNoise(baseRec.T2, rng)
		t2B.Append(t2)
		t24 := applyNoise(baseRec.T24, rng)
		t24B.Append(t24)
		t30 := applyNoise(baseRec.T30, rng)
		t30B.Append(t30)
		t50 := applyNoise(baseRec.T50, rng)
		t50B.Append(t50)
		p2 := applyNoise(baseRec.P2, rng)
		p2B.Append(p2)
		p15 := applyNoise(baseRec.P15, rng)
		p15B.Append(p15)
		p30 := applyNoise(baseRec.P30, rng)
		p30B.Append(p30)
		nf := applyNoise(baseRec.Nf, rng)
		nfB.Append(nf)
		nc := applyNoise(baseRec.Nc, rng)
		ncB.Append(nc)
		epr := applyNoise(baseRec.Epr, rng)
		eprB.Append(epr)
		ps30 := applyNoise(baseRec.Ps30, rng)
		ps30B.Append(ps30)
		phi := applyNoise(baseRec.Phi, rng)
		phiB.Append(phi)
		nrf := applyNoise(baseRec.NRf, rng)
		nrfB.Append(nrf)
		nrc := applyNoise(baseRec.NRc, rng)
		nrcB.Append(nrc)
		bpr := applyNoise(baseRec.BPR, rng)
		bprB.Append(bpr)
		farB := applyNoise(baseRec.FarB, rng)
		farBB.Append(farB)
		ht := applyNoise(baseRec.HtBleed, rng)
		htBleedB.Append(ht)
		nfd := applyNoise(baseRec.Nf_dmd, rng)
		nfdmdB.Append(nfd)
		pcn := applyNoise(baseRec.PCNfR_dmd, rng)
		pcnfrdmdB.Append(pcn)
		w31 := applyNoise(baseRec.W31, rng)
		w31B.Append(w31)
		w32 := applyNoise(baseRec.W32, rng)
		w32B.Append(w32)

		// 4. SATCOM TRANSMISSION (Kafka MVP)
		if transmit != nil {
			transmit(database.TelemetryFrame{
				Timestamp: tickTime.Format(time.RFC3339Nano),
				FlightRecord: database.FlightRecord{
					UnitNumber: baseRec.UnitNumber,
					TimeCycles: baseRec.TimeCycles,
					OpSetting1: op1,
					OpSetting2: op2,
					OpSetting3: baseRec.OpSetting3,
					T2: t2, T24: t24, T30: t30, T50: t50,
					P2: p2, P15: p15, P30: p30,
					Nf: nf, Nc: nc, Epr: epr, Ps30: ps30,
					Phi: phi, NRf: nrf, NRc: nrc, BPR: bpr,
					FarB: farB, HtBleed: ht, Nf_dmd: nfd,
					PCNfR_dmd: pcn, W31: w31, W32: w32,
				},
				ProcessingAt: time.Now().UTC(),
			})
		}
	}

	return nil
}
