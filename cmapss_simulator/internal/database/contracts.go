package database

import "time"

// FlightRecord — строгий дата-контракт L2.
// Отражает одну строку физики из транзакционной базы SQLite.
// AI-Ready: Все типы жестко зафиксированы (int32, float64).
type FlightRecord struct {
	UnitNumber int32 `json:"unit_number"`
	TimeCycles int32 `json:"time_cycles"`

	// Операционные настройки
	OpSetting1 float64 `json:"op_setting_1"`
	OpSetting2 float64 `json:"op_setting_2"`
	OpSetting3 float64 `json:"op_setting_3"`

	// Датчики NASA (21 шт.)
	T2        float64 `json:"T2"`
	T24       float64 `json:"T24"`
	T30       float64 `json:"T30"`
	T50       float64 `json:"T50"`
	P2        float64 `json:"P2"`
	P15       float64 `json:"P15"`
	P30       float64 `json:"P30"`
	Nf        float64 `json:"Nf"`
	Nc        float64 `json:"Nc"`
	Epr       float64 `json:"epr"`
	Ps30      float64 `json:"Ps30"`
	Phi       float64 `json:"phi"`
	NRf       float64 `json:"NRf"`
	NRc       float64 `json:"NRc"`
	BPR       float64 `json:"BPR"`
	FarB      float64 `json:"farB"`
	HtBleed   float64 `json:"htBleed"`
	Nf_dmd    float64 `json:"Nf_dmd"`
	PCNfR_dmd float64 `json:"PCNfR_dmd"`
	W31       float64 `json:"W31"`
	W32       float64 `json:"W32"`
}

// TelemetryFrame — пакет данных для Kafka (Stage 3).
// Включает состояние двигателя и метку времени обработки.
type TelemetryFrame struct {
	Timestamp string `json:"timestamp"`
	FlightRecord
	ProcessingAt time.Time `json:"processing_at"`
}

// TakeoffRequest — запрос от горутины-самолета на получение следующего полетного цикла.
type TakeoffRequest struct {
	UnitNumber int32
	RespChan   chan<- TakeoffResponse
}

// TakeoffResponse — ответ Диспетчера с выданным заданием на полет.
type TakeoffResponse struct {
	HasMoreFlights    bool
	Record            FlightRecord
	StartTime         time.Time
	TargetDurationSec int
}

// LandingReport — отчет от горутины-самолета о завершении или прерывании полета.
type LandingReport struct {
	UnitNumber int32
	TimeCycles int32
	Status     string
}