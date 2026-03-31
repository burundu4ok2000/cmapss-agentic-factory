package storage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// ==============================================================================
// STORAGE WRITER (I/O & Memory Shield)
// AI-Ready Standard:
// 1. Изоляция C-аллокатора (Защита от утечек и Segfault).
// 2. Атомарные транзакции на файловой системе (Zero-Corruption I/O).
// 3. WORM-совместимость (Write Once, Read Many).
// ==============================================================================

// RecordBuilder — обертка над Arrow Builder для ручного управления памятью.
// Инкапсулирует сложность C-аллокатора. ИИ-агентам запрещено напрямую работать
// с builder.builder вне этой абстракции.
type RecordBuilder struct {
	builder *array.RecordBuilder
	schema  *arrow.Schema
	mem     memory.Allocator
	// flag для защиты от двойного освобождения памяти
	released bool 
}

// NewRecordBuilder создает новый инстанс билдера с предварительной аллокацией.
func NewRecordBuilder(targetRows int) *RecordBuilder {
	mem := memory.NewGoAllocator() // Используем стандартный Go аллокатор (удобнее для Edge GC)
	schema := GetTelemetrySchema()
	builder := array.NewRecordBuilder(mem, schema)

	// 🚨 ПАРАНОЙЯ L1 (Pre-allocation Guard):
	// Резервируем память ДО начала цикла физики. Это исключает фрагментацию памяти
	// и предотвращает сотни мелких сисколлов (syscalls) к ядру Linux на выделение RAM.
	builder.Reserve(targetRows)

	return &RecordBuilder{
		builder:  builder,
		schema:   schema,
		mem:      mem,
		released: false,
	}
}

// GetBuilder возвращает оригинальный Arrow Builder для заполнения колонками.
func (rb *RecordBuilder) GetBuilder() *array.RecordBuilder {
	return rb.builder
}

// Release очищает C-подобную память.
// 🚨 ПАРАНОЙЯ L2 (Idempotent Release): ИИ-агенты могут по ошибке вызвать defer дважды.
// Функция проверяет флаг released, предотвращая критическую панику (Double Free).
func (rb *RecordBuilder) Release() {
	if !rb.released && rb.builder != nil {
		rb.builder.Release()
		rb.released = true
	}
}

// WriteParquetAtomically сохраняет Arrow Record в файл с максимальной паранойей.
// Обеспечивает строгий ACID (Atomicity) на уровне POSIX файловой системы.
func WriteParquetAtomically(record arrow.RecordBatch, targetPath string, compressionLevel int) error {
	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("не удалось создать директории %s: %w", dir, err)
	}

	// 🚨 ПАРАНОЙЯ L3 (Atomic File Protocol): Мы пишем только во временный скрытый файл.
	// Spark (или другой Reader) не увидит файл, пока он не будет полностью готов.
	tmpPath := targetPath + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("не удалось создать временный файл %s: %w", tmpPath, err)
	}

	writeSuccess := false

	// 🚨 ПАРАНОЙЯ L4 (Defensive Cleanup): Абсолютная гарантия отсутствия мусора.
	// Если функция завершится паникой или ошибкой до флага writeSuccess=true, 
	// ОС закроет дескриптор, а недописанный файл будет немедленно стерт.
	defer func() {
		file.Close() // Идемпотентное закрытие (игнорируем ошибку, если уже закрыт)
		if !writeSuccess {
			os.Remove(tmpPath)
			log.Printf("[I/O ROLLBACK] Запись прервана. Временный файл %s удален.", tmpPath)
		}
	}()

	// 🚨 ПАРАНОЙЯ L5 (Memory Shield: Parquet RowGroups):
	// Если не ограничить размер RowGroup, pqarrow попытается буферизовать весь 
	// 2-часовой полет (сотни МБ) в RAM перед компрессией. Asus упадет от OOM.
	// Мы принудительно сбрасываем данные на диск блоками по 8192 строк (Streaming Compression).
	props := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),            // Отключаем словари для Float-рядов (экономит 40% RAM)
		parquet.WithDataPageVersion(parquet.DataPageV2), // Стандарт Iceberg/Spark
		parquet.WithCompression(compress.Codecs.Snappy), // Быстрое сжатие без CGO
		parquet.WithMaxRowGroupLength(8192),             // ⚡ КРИТИЧЕСКИЙ ФИКС OOM ⚡
	)

	// Инициализация Parquet Writer
	writer, err := pqarrow.NewFileWriter(record.Schema(), file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return fmt.Errorf("ошибка инициализации Parquet Writer: %w", err)
	}

	// Запись батча (компрессия происходит порциями благодаря WithMaxRowGroupLength)
	if err := writer.Write(record); err != nil {
		writer.Close()
		return fmt.Errorf("ошибка записи Record: %w", err)
	}

	// Закрытие Writer'а сбрасывает Footer и метаданные
	if err := writer.Close(); err != nil {
		return fmt.Errorf("ошибка записи Footer Parquet: %w", err)
	}

	// 🚨 ПАРАНОЙЯ L6 (Fsync): Принудительный сброс кэшей ядра Linux на физический диск.
	// Защита от потери данных при отключении электричества (Hard Power Loss).
	if err := file.Sync(); err != nil {
		return fmt.Errorf("ошибка fsync (диск не принял данные): %w", err)
	}

	// Закрываем файл ДО переименования (обязательно для Windows/NFS)
	if err := file.Close(); err != nil {
		return fmt.Errorf("ошибка закрытия дескриптора: %w", err)
	}

	// 🚨 ПАРАНОЙЯ L7 (Atomic Rename): Превращаем .tmp в рабочий .parquet.
	// Это атомарная POSIX операция. Spark увидит файл мгновенно и целиком.
	if err := os.Rename(tmpPath, targetPath); err != nil {
		return fmt.Errorf("ошибка атомарного переименования %s -> %s: %w", tmpPath, targetPath, err)
	}

	// Рубикон пройден. Мусорщик (defer) не удалит рабочий файл.
	writeSuccess = true
	return nil
}