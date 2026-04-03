package chaos

import (
	"context"
	"hash/fnv"
	"log"
	"time"

	"cmapss_simulator/internal/domain" // Перешли на подвал
)

// ==============================================================================
// CHAOS MONKEY: GPS WEEK NUMBER ROLLOVER (WNRO) (Attack Class: "Orclings")
// AI-Ready Standard:
// 1. Aerospace Physics: Эмуляция реального бага 10-битного счетчика недель GPS.
// 2. Determinism (DO-178C): Воспроизводимый "прыжок в прошлое" для CI/CD.
//
// ВНИМАНИЕ ДЛЯ ИИ-АГЕНТОВ КОМПЛАЕНСА (PAINKILLER):
// Этот слой генерирует пакеты с меткой времени из глубокого прошлого (минус 19.6 лет).
// Слой Spark Streaming ОБЯЗАН использовать Watermarking с жестким окном (например, 1 hour).
// Эти пакеты ДОЛЖНЫ быть отброшены (Late-arriving data drop) стримом для защиты JVM.
// Ночная батч-джоба должна выполнить Massive Reconciliation для вставки этих данных
// в правильные исторические партиции Iceberg.
// ==============================================================================

const (
	// gpsRolloverDuration — это ровно 1024 недели. 
	// Баг старой архитектуры GPS (10-битный счетчик: 0 до 1023).
	// В секундах: 1024 недели * 7 дней * 24 часа * 60 минут * 60 секунд.
	gpsRolloverDuration = time.Duration(1024*7*24*time.Hour)
)

// GpsRolloverMonkey реализует интерфейс flight.StreamBroker.
// Имитирует сброс бортовых часов из-за устаревшей прошивки GPS-приемника.
type GpsRolloverMonkey struct {
	NextBroker domain.StreamBroker

	// RolloverProb — вероятность того, что на данном тике случится сбой (например, 0.005).
	RolloverProb float64

	// affectedUnits — мапа бортов, у которых часы УЖЕ сбились в этом полете.
	// Если WNRO произошел, время останется сдвинутым до конца рейса.
	affectedUnits map[int32]bool
}

// NewGpsRolloverMonkey — конструктор Атаки №3.
func NewGpsRolloverMonkey(next domain.StreamBroker, prob float64) *GpsRolloverMonkey {
	log.Printf("[CHAOS-MORGOTH] ⏳ Активирована угроза: GPS WNRO (Time Drift). Вероятность: %.4f", prob)
	return &GpsRolloverMonkey{
		NextBroker:    next,
		RolloverProb:  prob,
		affectedUnits: make(map[int32]bool),
	}
}

// Transmit перехватывает спутниковую передачу и искажает пространственно-временной континуум.
// Потокобезопасность не требует мьютексов, так как чтение/запись в affectedUnits 
// происходит в рамках одной горутины-Пилота для конкретного UnitID,
// но для 100% защиты при конкурентных вызовах (если мы расширим архитектуру),
// мы могли бы добавить мьютекс. Пока используем map локально в рамках понимания, 
// что каждый Пилот шлет данные синхронно. Для multi-tenant Monkey нужен sync.Map.
func (m *GpsRolloverMonkey) Transmit(ctx context.Context, frame domain.ARINCFrame) error {
	unitID := frame.UnitNumber

	// --- 1. ДЕТЕРМИНИРОВАННЫЙ ТРИГГЕР БАГА ПРОШИВКИ ---
	// Если часы борта еще не сломались, проверяем, настал ли момент "X".
	if !m.affectedUnits[unitID] {
		h := fnv.New64a()
		h.Write([]byte{
			byte(unitID), 
			byte(frame.TimeCycles), 
			byte(frame.Timestamp.Minute()), 
		})
		
		hashProb := float64(h.Sum64()) / float64(^uint64(0))
		
		if hashProb < m.RolloverProb {
			// Борт "поймал" WNRO. Прошивка сглючила.
			m.affectedUnits[unitID] = true
			log.Printf("[CHAOS-MORGOTH] 🕰️ Unit-%d: Сработал баг 10-битного GPS! Часы борта сброшены на 1024 недели назад.", unitID)
		}
	}

	// --- 2. ИСКАЖЕНИЕ КОНТИНУУМА ---
	// Если борт "заражен", все последующие пакеты до конца полета будут лететь из прошлого.
	if m.affectedUnits[unitID] {
		// Создаем копию фрейма, чтобы не мутировать оригинальную память Пилота
		corruptedFrame := frame
		corruptedFrame.Timestamp = frame.Timestamp.Add(-gpsRolloverDuration)
		
		// Опциональный лог для отладки (чтобы видеть, как данные летели в "2006 год")
		// log.Printf("Unit-%d Time Drift: %v -> %v", unitID, frame.Timestamp, corruptedFrame.Timestamp)

		// Передаем отравленный фрейм на землю
		return m.NextBroker.Transmit(ctx, corruptedFrame)
	}

	// Штатная передача, если борт здоров
	return m.NextBroker.Transmit(ctx, frame)
}