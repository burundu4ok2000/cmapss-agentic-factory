package database

import "time"

// FlightRecord — строгий дата-контракт L2.
// Отражает одну строку физики из транзакционной базы SQLite.
// AI-Ready: Все типы жестко зафиксированы (int32, float64).
type FlightRecord struct {
	UnitNumber int32
	TimeCycles int32

	// Операционные настройки
	OpSetting1 float64
	OpSetting2 float64
	OpSetting3 float64

	// Датчики NASA (21 шт.)
	T2        float64
	T24       float64
	T30       float64
	T50       float64
	P2        float64
	P15       float64
	P30       float64
	Nf        float64
	Nc        float64
	Epr       float64
	Ps30      float64
	Phi       float64
	NRf       float64
	NRc       float64
	BPR       float64
	FarB      float64
	HtBleed   float64
	Nf_dmd    float64
	PCNfR_dmd float64
	W31       float64
	W32       float64
}

// TakeoffRequest — запрос от горутины-самолета на получение следующего полетного цикла.
type TakeoffRequest struct {
	UnitNumber int32
	// RespChan — канал обратной связи (защита от Deadlock).
	// Каждая горутина передает свой личный канал для получения ответа от Диспетчера.
	RespChan chan<- TakeoffResponse
}

// TakeoffResponse — ответ Диспетчера с выданным заданием на полет.
type TakeoffResponse struct {
	HasMoreFlights    bool         // False, если у турбины больше нет PENDING циклов (взрыв)
	Record            FlightRecord // Эталонные значения датчиков
	StartTime         time.Time    // Системный (UTC) или исторический якорь времени
	TargetDurationSec int          // Рандомизированная длительность полета
}

// LandingReport — отчет от горутины-самолета о завершении или прерывании полета.
type LandingReport struct {
	UnitNumber int32
	TimeCycles int32
	Status     string // Допустимые значения: "COMPLETED", "FAILED", "INTERRUPTED"
}