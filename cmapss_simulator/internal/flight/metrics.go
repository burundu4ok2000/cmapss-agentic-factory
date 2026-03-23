package flight

import (
	"sync/atomic"
)

// SharedMetrics — структура для сбора телеметрии работы самого симулятора.
// AI-Ready: Использование atomic гарантирует нулевые блокировки памяти (Lock-Free).
type SharedMetrics struct {
	TotalFlights    atomic.Uint64
	PointsGenerated atomic.Uint64
	FilesSaved      atomic.Uint64
	Errors          atomic.Uint64
	ActivePilots    atomic.Int32
}