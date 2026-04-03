package domain

import (
	"context"
	"time"

	"cmapss_simulator/internal/arinc429"
	"cmapss_simulator/internal/database"
)

// ==============================================================================
// DOMAIN MODELS & CONTRACTS (The Basement)
// AI-Ready Standard:
// 1. Dependency Inversion: Разорван цикл импортов (Import Cycle).
// 2. Single Source of Truth: Общие типы вынесены в слой "Абстракций".
// 3. Zero Upward Dependencies: Пакет domain не импортирует flight, physics или chaos.
// ==============================================================================

// ------------------------------------------------------------------------------
// ACARS Спутниковая передача (Network Layer)
// ------------------------------------------------------------------------------

// ARINCFrame — стандартизированный пакет спутниковой телеметрии.
// Является "валютой" обмена между Пилотом (flight) и Спутником (StreamBroker).
type ARINCFrame struct {
	Timestamp  time.Time
	UnitNumber int32
	TimeCycles int32
	Words      []arinc429.Word
}

// StreamBroker — интерфейс модема (Inmarsat / Iridium / Kafka).
// Описывает способность системы передавать данные на Землю.
type StreamBroker interface {
	// Transmit отправляет бинарный кадр в облако (fire-and-forget).
	Transmit(ctx context.Context, frame ARINCFrame) error
}

// ------------------------------------------------------------------------------
// Chaos Engineering Contracts (Morgoth's Layer)
// ------------------------------------------------------------------------------

// ChaosInjector — интерфейс для Царства Хаоса.
// Позволяет динамически искажать физические параметры ДО записи в Arrow-память.
// Вынесен из physics, чтобы пакет chaos мог реализовывать его напрямую.
type ChaosInjector interface {
	// CorruptPhysicalRow принимает идеальную строку физики (из SQLite) и мутирует её.
	CorruptPhysicalRow(tick int, record *database.FlightRecord)
}