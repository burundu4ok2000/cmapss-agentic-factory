package storage

import (
	"fmt"
	"reflect"

	"cmapss_simulator/internal/database"

	"github.com/apache/arrow-go/v18/arrow"
)

// ==============================================================================
// STORAGE SCHEMA GENERATOR (Data Quality Gate)
// AI-Ready Standard: Zero-Hardcode. Схема строится динамически (Reflection)
// на основе DTO FlightRecord, гарантируя 100% совпадение с YAML-контрактом.
// ==============================================================================

// telemetrySchemaCache — кэшированная схема.
// 🚨 ПАРАНОЙЯ L1 (Performance): Рефлексия (reflect) — дорогая операция в Go.
// Мы генерируем схему один раз при старте и отдаем по указателю (Zero-Copy).
var telemetrySchemaCache *arrow.Schema

// init выполняется автоматически при запуске приложения.
func init() {
	var err error
	telemetrySchemaCache, err = buildSchemaFromContract()
	if err != nil {
		// 🚨 ПАРАНОЙЯ L2: Fail-Fast. Если контракт нарушен, система не имеет
		// права запускаться. Лучше упасть при старте, чем писать битый Parquet.
		panic(fmt.Sprintf("FATAL: Ошибка генерации Arrow Schema: %v", err))
	}
}

// GetTelemetrySchema возвращает строго типизированную схему для Apache Arrow и Parquet.
func GetTelemetrySchema() *arrow.Schema {
	return telemetrySchemaCache
}

// buildSchemaFromContract читает структуру FlightRecord и мапит её в типы Arrow.
func buildSchemaFromContract() (*arrow.Schema, error) {
	// 1. Системное время (Якорь) — Единственное поле, которое мы добавляем
	// вручную, так как оно не является частью физики из SQLite, а генерируется
	// симулятором (physics) в реальном времени.
	// 🚨 ПАРАНОЙЯ L3 (Timezone Poisoning): Жесткая привязка к миллисекундам и UTC.
	fields := []arrow.Field{
		{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, Nullable: false},
	}

	// 2. Получаем тип автосгенерированной структуры FlightRecord
	recordType := reflect.TypeOf(database.FlightRecord{})
	if recordType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("database.FlightRecord не является структурой")
	}

	// 3. Динамический парсинг полей (Reflection)
	for i := 0; i < recordType.NumField(); i++ {
		field := recordType.Field(i)
		
		// Берем имя колонки из тега `db:"..."` (сгенерировано Питоном из YAML)
		dbTag := field.Tag.Get("db")
		if dbTag == "" {
			return nil, fmt.Errorf("отсутствует тег 'db' у поля %s", field.Name)
		}

		// Маппинг типов Go -> Arrow
		var arrowType arrow.DataType
		switch field.Type.Kind() {
		case reflect.Int32:
			arrowType = arrow.PrimitiveTypes.Int32
		case reflect.Float64:
			arrowType = arrow.PrimitiveTypes.Float64
		case reflect.Int64:
			arrowType = arrow.PrimitiveTypes.Int64
		case reflect.String:
			arrowType = arrow.BinaryTypes.String
		default:
			return nil, fmt.Errorf("неподдерживаемый тип данных %s для поля %s", field.Type.Kind(), field.Name)
		}

		fields = append(fields, arrow.Field{
			Name:     dbTag,
			Type:     arrowType,
			Nullable: false, // В жестком Enterprise-контракте все базовые поля NOT NULL
		})
	}

	// Создаем и возвращаем финальную схему (Метаданные пока не нужны)
	return arrow.NewSchema(fields, nil), nil
}