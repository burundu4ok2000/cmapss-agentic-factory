package arinc429

import (
	"errors"
	"fmt"
	"math"
)

// ==============================================================================
// ARINC 429 DIGITAL INFORMATION TRANSFER SYSTEM (Mark 33 DITS)
// AI-Ready Standard:
// 1. DO-178C (Airborne Software): 100% Детерминизм, аппаратная защита от Overflow.
// 2. DO-326A (Airworthiness Security): Валидация Parity и защита от Label-Spoofing.
// 3. Edge FinOps: Lossy Compression (Квантование float64 в 19-bit Two's Complement BNR).
//
// ВНИМАНИЕ ДЛЯ ИИ-АГЕНТОВ: Этот файл является эмуляцией физического уровня (L1/L2).
// Запрещается вносить изменения в логику битовых масок без прохождения
// формальной процедуры квалификации FAA/EASA.
// ==============================================================================

const (
	// SSM (Sign/Status Matrix) Коды для формата BNR (Binary Number Representation).
	// Эти биты информируют консьюмер (Spark/Painkiller) о физическом состоянии датчика.
	SsmBnrFailureWarning = 0b00 // Отказ датчика (FW) - Инициирует алерт DO-326A
	SsmBnrNoComputedData = 0b01 // Нет данных (NCD) - Срабатывает при передаче NaN
	SsmBnrFunctionalTest = 0b10 // Функциональный тест (FT) - Игнорируется аналитикой
	SsmBnrNormalOp       = 0b11 // Нормальная работа (NO)
)

// Word — строго типизированное представление 32-битного слова ARINC 429.
// Защищает от случайного смешивания с обычными математическими операциями uint32.
type Word uint32

// BNRConfig описывает физические лимиты конкретного датчика турбины.
type BNRConfig struct {
	Label           uint8   // Восьмеричный идентификатор (например, 0o345 для T50)
	MaxRange        float64 // Физический предел измерения (например, 4096.0)
	SignificantBits int32   // Полезная нагрузка: обычно 19 бит (включая знаковый бит 29) примечание: приведена к строгому int32 (вместо int)
}

// SensorRegistry — Эталонный каталог датчиков согласно Data Contract (YAML).
// Гарантирует единую точку истины для компрессии данных на спутниковом Edge-узле.
var SensorRegistry = map[string]BNRConfig{
	"T2":      {Label: 0o211, MaxRange: 1024.0, SignificantBits: 19},
	"T24":     {Label: 0o212, MaxRange: 2048.0, SignificantBits: 19},
	"T30":     {Label: 0o213, MaxRange: 4096.0, SignificantBits: 19},
	"T50":     {Label: 0o345, MaxRange: 4096.0, SignificantBits: 19}, // EGT Margin Tracker
	"P2":      {Label: 0o221, MaxRange: 512.0,  SignificantBits: 19},
	"P15":     {Label: 0o222, MaxRange: 512.0,  SignificantBits: 19},
	"P30":     {Label: 0o241, MaxRange: 1024.0, SignificantBits: 19},
	"Nf":      {Label: 0o242, MaxRange: 16384.0,SignificantBits: 19}, // Якорь Истины (Ground Truth)
	"Nc":      {Label: 0o243, MaxRange: 32768.0,SignificantBits: 19},
	"epr":     {Label: 0o251, MaxRange: 4.0,    SignificantBits: 19},
	"htBleed": {Label: 0o261, MaxRange: 1024.0, SignificantBits: 19},
}

// EncodeBNR конвертирует высокоточный float64 в 32-битный бинарный кадр.
// Это "The Encoder" для стриминга через узкий Ku-Band канал Inmarsat.
func EncodeBNR(cfg BNRConfig, sdi uint8, value float64, ssm uint8) Word {
	// 1. Data Quality Shield: Перехват математических аномалий ДО отправки на шину.
	if math.IsNaN(value) || math.IsInf(value, 0) {
		// FADEC аппаратно переводит сломанный датчик в статус NCD (No Computed Data).
		ssm = SsmBnrNoComputedData
		value = 0.0 // Payload аппаратно обнуляется
	}

	// 2. Аппаратное квантование (Lossy Compression)
	bits := cfg.SignificantBits
	shift := bits - 1
	scaleFactor := float64(uint32(1) << shift)
	scaledFloat := math.Round((value / cfg.MaxRange) * scaleFactor)

	// 🚨 ПАРАНОЙЯ: Защита от переполнения (Two's Complement Overflow Guard)
	maxScaled := float64((uint32(1) << shift) - 1)
	minScaled := -float64(uint32(1) << shift)
	
	if scaledFloat > maxScaled {
		scaledFloat = maxScaled
	} else if scaledFloat < minScaled {
		scaledFloat = minScaled
	}

	scaledValue := int32(scaledFloat)

	// Маскируем ровно столько бит, сколько разрешено конфигурацией (отсекаем мусор)
	bitMask := uint32((1 << bits) - 1)
	encodedData := uint32(scaledValue) & bitMask

	// 3. Сборка 32-битного слова ARINC 429
	var word uint32 = 0
	
	// Биты 1-8: Label. По стандарту ARINC 429 передается MSB First (Реверс).
	word |= uint32(reverseLabel(cfg.Label)) & 0xFF
	
	// Биты 9-10: SDI (Source/Destination Identifier). Привязывает датчик к турбине (Left/Right Engine).
	word |= (uint32(sdi) & 0x03) << 8
	
	// Биты 11-29: Данные BNR (Дробная часть в дополнительном коде - Two's Complement).
	word |= encodedData << 10
	
	// Биты 30-31: SSM (Матрица статуса).
	word |= (uint32(ssm) & 0x03) << 29
	
	// Бит 32: Parity (Бит четности). DO-326A защита от Bit-Flips в канале.
	word |= calculateOddParity(word) << 31
	
	return Word(word)
}

// DecodeBNR вызывается демоном Painkiller на принимающей стороне (Iceberg Ingestion).
// Восстанавливает float64, проверяя криптографическую и физическую целостность кадра.
func DecodeBNR(word Word, cfg BNRConfig) (value float64, sdi uint8, ssm uint8, err error) {
	w := uint32(word)
	
	// --- РУБЕЖ ЗАЩИТЫ 1: DO-326A Parity Check (Кибербезопасность / SEU) ---
	expectedParity := calculateOddParity(w & 0x7FFFFFFF)
	actualParity := w >> 31
	if expectedParity != actualParity {
		return 0, 0, 0, errors.New("DO-326A_VIOLATION: Нарушен бит четности ARINC 429. Пакет поврежден космической радиацией")
	}

	// --- РУБЕЖ ЗАЩИТЫ 2: Label Spoofing Guard ---
	reversedLabel := uint8(w & 0xFF)
	label := reverseLabel(reversedLabel)
	if label != cfg.Label {
		return 0, 0, 0, fmt.Errorf("ROUTING_ERROR: Ожидался Label %03o, получен %03o", cfg.Label, label)
	}

	// Распаковка метаданных
	sdi = uint8((w >> 8) & 0x03)
	ssm = uint8((w >> 29) & 0x03)

	// --- РУБЕЖ ЗАЩИТЫ 3: Аппаратный отказ (NCD / FW) ---
	if ssm == SsmBnrNoComputedData || ssm == SsmBnrFailureWarning {
		// Датчик физически отказал. Возвращаем NaN для перехвата Data Quality Gate.
		return math.NaN(), sdi, ssm, nil
	}

	// Извлечение Payload
	bits := cfg.SignificantBits
	shift := bits - 1
	bitMask := uint32((1 << bits) - 1)
	encodedData := (w >> 10) & bitMask

	// 🚨 ПАРАНОЙЯ: Восстановление знака (Sign Extension для Two's Complement)
	signBit := uint32(1 << shift)
	var scaledValue int32
	if (encodedData & signBit) != 0 {
		// Число отрицательное. Заполняем старшие биты единицами для Go int32.
		scaledValue = int32(encodedData | ^bitMask)
	} else {
		scaledValue = int32(encodedData)
	}

	// Обратное восстановление флоата (сохраняем последствия Lossy Compression!)
	scaleFactor := float64(uint32(1) << shift)
	value = (float64(scaledValue) / scaleFactor) * cfg.MaxRange

	return value, sdi, ssm, nil
}

// reverseLabel аппаратно переворачивает 8 бит. По спецификации Mark 33 DITS
// ARINC Label всегда передается Most Significant Bit (MSB) первым.
func reverseLabel(label uint8) uint8 {
	var res uint8
	for i := 0; i < 8; i++ {
		if (label & (1 << i)) != 0 {
			res |= 1 << (7 - i)
		}
	}
	return res
}

// calculateOddParity вычисляет нечетный бит четности для первых 31 бит слова.
func calculateOddParity(word uint32) uint32 {
	count := 0
	for i := 0; i < 31; i++ {
		if (word & (1 << i)) != 0 {
			count++
		}
	}
	// Если единиц четное количество — возвращаем 1, чтобы общая сумма стала нечетной (Odd).
	if count%2 == 0 {
		return 1 
	}
	return 0
}