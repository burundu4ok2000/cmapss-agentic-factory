package storage

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// GetTelemetrySchema возвращает строго типизированную схему для Apache Arrow и Parquet.
// AI-Ready: Схема фиксируется один раз. Гарантирует совместимость со Spark/Iceberg.
// 🚨 ПАРАНОЙЯ L1 (Timezone Poisoning): Поле timestamp жестко привязано к миллисекундам и зоне "UTC".
func GetTelemetrySchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			// 1. Системное время (Якорь)
			{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, Nullable: false},

			// 2. Идентификаторы (INT32)
			{Name: "unit_number", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "time_cycles", Type: arrow.PrimitiveTypes.Int32, Nullable: false},

			// 3. Операционные настройки (FLOAT64)
			{Name: "op_setting_1", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "op_setting_2", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "op_setting_3", Type: arrow.PrimitiveTypes.Float64, Nullable: false},

			// 4. Датчики NASA (21 штука, FLOAT64) - Описание взято из предоставленного CSV
			{Name: "T2", Type: arrow.PrimitiveTypes.Float64, Nullable: false},      // Полная температура на входе вентилятора
			{Name: "T24", Type: arrow.PrimitiveTypes.Float64, Nullable: false},     // Полная температура на выходе LPC
			{Name: "T30", Type: arrow.PrimitiveTypes.Float64, Nullable: false},     // Полная температура на выходе HPC
			{Name: "T50", Type: arrow.PrimitiveTypes.Float64, Nullable: false},     // Полная температура на выходе LPT
			{Name: "P2", Type: arrow.PrimitiveTypes.Float64, Nullable: false},      // Давление на входе вентилятора
			{Name: "P15", Type: arrow.PrimitiveTypes.Float64, Nullable: false},     // Полное давление в байпасном канале
			{Name: "P30", Type: arrow.PrimitiveTypes.Float64, Nullable: false},     // Полное давление на выходе HPC
			{Name: "Nf", Type: arrow.PrimitiveTypes.Float64, Nullable: false},      // Физическая скорость вращения вентилятора
			{Name: "Nc", Type: arrow.PrimitiveTypes.Float64, Nullable: false},      // Физическая скорость вращения ядра
			{Name: "epr", Type: arrow.PrimitiveTypes.Float64, Nullable: false},     // Степень повышения давления
			{Name: "Ps30", Type: arrow.PrimitiveTypes.Float64, Nullable: false},    // Статическое давление на выходе HPC
			{Name: "phi", Type: arrow.PrimitiveTypes.Float64, Nullable: false},     // Отношение расхода топлива к статическому давлению
			{Name: "NRf", Type: arrow.PrimitiveTypes.Float64, Nullable: false},     // Скорректированная скорость вращения вентилятора
			{Name: "NRc", Type: arrow.PrimitiveTypes.Float64, Nullable: false},     // Скорректированная скорость вращения ядра
			{Name: "BPR", Type: arrow.PrimitiveTypes.Float64, Nullable: false},     // Bypass Ratio
			{Name: "farB", Type: arrow.PrimitiveTypes.Float64, Nullable: false},    // Burner fuel-air ratio
			{Name: "htBleed", Type: arrow.PrimitiveTypes.Float64, Nullable: false}, // Bleed Enthalpy
			{Name: "Nf_dmd", Type: arrow.PrimitiveTypes.Float64, Nullable: false},  // Demanded fan speed
			{Name: "PCNfR_dmd", Type: arrow.PrimitiveTypes.Float64, Nullable: false},// Demanded corrected fan speed
			{Name: "W31", Type: arrow.PrimitiveTypes.Float64, Nullable: false},     // HPT coolant bleed
			{Name: "W32", Type: arrow.PrimitiveTypes.Float64, Nullable: false},     // LPT coolant bleed
		},
		nil, // Метаданные (пока не нужны)
	)
}