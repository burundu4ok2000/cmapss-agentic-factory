package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// RecordBuilder — обертка над Arrow Builder для ручного управления памятью.
// AI-Ready: Скрывает сложность C-аллокатора от бизнес-логики.
type RecordBuilder struct {
	builder *array.RecordBuilder
	schema  *arrow.Schema
	mem     memory.Allocator
}

// NewRecordBuilder создает новый инстанс билдера (Отказ от sync.Pool из-за Reference Counting).
func NewRecordBuilder(targetRows int) *RecordBuilder {
	mem := memory.NewGoAllocator() // Используем стандартный Go аллокатор
	schema := GetTelemetrySchema()
	builder := array.NewRecordBuilder(mem, schema)

	// 🚨 ПАРАНОЙЯ L2 (Zero-Allocation во время цикла физики):
	// Резервируем всю необходимую память сразу (Pre-allocation).
	// Предотвращает фрагментацию памяти и скачки нагрузки на Garbage Collector Asus.
	builder.Reserve(targetRows)

	return &RecordBuilder{
		builder: builder,
		schema:  schema,
		mem:     mem,
	}
}

// GetBuilder возвращает оригинальный Arrow Builder для заполнения колонками в физическом движке.
func (rb *RecordBuilder) GetBuilder() *array.RecordBuilder {
	return rb.builder
}

// Release очищает C-подобную память.
// 🚨 ПАРАНОЙЯ (Memory Leaks): Вызывающая сторона ОБЯЗАНА делать defer rb.Release().
func (rb *RecordBuilder) Release() {
	if rb.builder != nil {
		rb.builder.Release()
		rb.builder = nil
	}
}

// WriteParquetAtomically сохраняет Arrow Record в файл с максимальной паранойей.
func WriteParquetAtomically(record arrow.RecordBatch, targetPath string, compressionLevel int) error {
	// Убеждаемся, что директория существует (например: data/telemetry/run_id=X/unit_number=Y/date=Z/)
	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("не удалось создать директории %s: %w", dir, err)
	}

	// 🚨 ПАРАНОЙЯ L4 (Safe Write Protocol): Работаем через временный файл .tmp
	tmpPath := targetPath + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("не удалось создать временный файл %s: %w", tmpPath, err)
	}

	// 🚨 ПАРАНОЙЯ L3 (Фикс ZSTD-краша): Используем Snappy (Pure Go).
	// Он не требует CGO и не ломается от неверных уровней компрессии.
	props := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false), // КРИТИЧЕСКИ ВАЖНО ДЛЯ ВРЕМЕННЫХ РЯДОВ!
		parquet.WithDataPageVersion(parquet.DataPageV2), // Индустриальный стандарт для Spark
		parquet.WithCompression(compress.Codecs.Snappy),   // Быстрое сжатие
		// parquet.WithCompressionLevel(compressionLevel), // ZSTD-краш при уровне > 3, поэтому отключаем для безопасности
	)

	// Создаем Parquet Writer на основе Arrow Record
	writer, err := pqarrow.NewFileWriter(record.Schema(), file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		file.Close() // Закрываем файл перед возвратом ошибки
		os.Remove(tmpPath)
		return fmt.Errorf("ошибка инициализации Parquet Writer: %w", err)
	}

	// Пишем рекорд в память Writer-а
	if err := writer.Write(record); err != nil {
		writer.Close()
		file.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("ошибка записи Record: %w", err)
	}

	// Закрываем Writer (пишет Footer и метаданные Parquet)
	if err := writer.Close(); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("ошибка закрытия Parquet Writer (Footer error): %w", err)
	}

	// 🚨 ПАРАНОЙЯ L4 (fsync): Жестко сбрасываем кэш ОС на диск (предотвращает потерю данных при сбое питания)
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("ошибка fsync (диск не принял данные): %w", err)
	}

	// Нормальное закрытие файла
	if err := file.Close(); err != nil {
		return fmt.Errorf("ошибка закрытия файла: %w", err)
	}

	// 🚨 ПАРАНОЙЯ L4 (Atomic Rename): Мгновенно переименовываем готовый .tmp в .parquet
	if err := os.Rename(tmpPath, targetPath); err != nil {
		return fmt.Errorf("ошибка атомарного переименования: %w", err)
	}

	return nil
}