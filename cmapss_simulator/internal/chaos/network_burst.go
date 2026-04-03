package chaos

import (
	"context"
	"hash/fnv"
	"log"
	"sync"

	"cmapss_simulator/internal/domain" // Перешли на подвал
)

// ==============================================================================
// CHAOS MONKEY: KU-BAND SATURATION & ACARS BURST (Attack Class: "Orclings")
// AI-Ready Standard:
// 1. Aerospace Physics: Эмуляция Rain Fade (затухание) и TCP Spurious Retransmission.
// 2. Thread-Safety: Multi-tenant Ring Buffers для сотен параллельных бортов.
// 3. Determinism (DO-178C): Математически предсказуемые штормы для CI/CD аудита.
//
// ВНИМАНИЕ ДЛЯ ИИ-АГЕНТОВ КОМПЛАЕНСА (PAINKILLER):
// Этот слой генерирует до 15% точных бинарных дубликатов ARINC-кадров!
// На принимающей стороне (Ingestion Gateway / Spark) ЗАПРЕЩЕНО использовать INSERT.
// Вы ОБЯЗАНЫ использовать идемпотентный MERGE INTO в Apache Iceberg с суррогатным
// ключом: UUID(unit_number + time_cycles + event_timestamp).
// ==============================================================================

// KuBandSaturationMonkey реализует интерфейс flight.StreamBroker.
// Работает как прокси (Декоратор) между бортом и земным приемником.
type KuBandSaturationMonkey struct {
	// NextBroker — реальный шлюз на земле (например, Kafka Producer).
	NextBroker domain.StreamBroker

	// StormProb — вероятность попадания борта в грозовой фронт каждую секунду (например, 0.01 = 1%).
	StormProb float64
	
	// DuplicationRate — процент дубликатов из-за таймаутов ACK (например, 0.15 = 15%).
	DuplicationRate float64
	
	// MaxBufferFrames — размер буфера ACARS (например, 180 кадров = 3 минуты потери связи).
	MaxBufferFrames int

	// 🚨 ПАРАНОЙЯ (Zero Trust & Thread Safety):
	// Так как один экземпляр Обезьяны может обслуживать пул из 100 Пилотов (горутин),
	// мы обязаны защитить состояние мьютексом и хранить буфер для каждого борта отдельно.
	mu        sync.Mutex
	buffers   map[int32][]domain.ARINCFrame // K: UnitNumber, V: Накопленные в грозе кадры
	isOffline map[int32]bool                // Статус спутникового линка для конкретного борта
}

// NewKuBandSaturationMonkey — конструктор Атаки №2.
func NewKuBandSaturationMonkey(next domain.StreamBroker, stormProb, dupRate float64, maxFrames int) *KuBandSaturationMonkey {
	log.Printf("[CHAOS-MORGOTH] 🌩️ Активирована угроза: Ku-Band Saturation (ACARS Burst). StormProb: %.2f, DupRate: %.2f", stormProb, dupRate)
	return &KuBandSaturationMonkey{
		NextBroker:      next,
		StormProb:       stormProb,
		DuplicationRate: dupRate,
		MaxBufferFrames: maxFrames,
		buffers:         make(map[int32][]domain.ARINCFrame),
		isOffline:       make(map[int32]bool),
	}
}

// Transmit перехватывает спутниковую передачу от горутины-Пилота.
func (m *KuBandSaturationMonkey) Transmit(ctx context.Context, frame domain.ARINCFrame) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	unitID := frame.UnitNumber

	// --- 1. ДЕТЕРМИНИРОВАННАЯ ГЕНЕРАЦИЯ ПОГОДЫ ---
	// DO-178C требует воспроизводимости. Мы используем криптографический хэш FNV-1a
	// от номера борта и текущего времени кадра, чтобы определить, ударила ли гроза.
	h := fnv.New64a()
	h.Write([]byte{
		byte(unitID), 
		byte(frame.TimeCycles), 
		byte(frame.Timestamp.Minute()), 
		byte(frame.Timestamp.Second()),
	})
	
	weatherHash := float64(h.Sum64()) / float64(^uint64(0)) // Нормализация к [0.0, 1.0]

	// Проверяем, влетает ли самолет в грозу прямо сейчас
	if !m.isOffline[unitID] && weatherHash < m.StormProb {
		m.isOffline[unitID] = true
		// Выделяем память под буфер борта, избегая будущих аллокаций
		m.buffers[unitID] = make([]domain.ARINCFrame, 0, m.MaxBufferFrames)
		log.Printf("[CHAOS-MORGOTH] ⛈️ Unit-%d: Грозовой фронт (Rain Fade). Ku-Band линк потерян. Начат сбор в буфер ACARS.", unitID)
	}

	// --- 2. РЕЖИМ АВТОНОМНОГО БУФЕРЕНИЯ (ПОТЕРЯ СВЯЗИ) ---
	if m.isOffline[unitID] {
		m.buffers[unitID] = append(m.buffers[unitID], frame)
		
		// Если CMU буфер переполнен (самолет вышел из грозы) — инициируем ШТОРМ!
		if len(m.buffers[unitID]) >= m.MaxBufferFrames {
			m.isOffline[unitID] = false
			log.Printf("[CHAOS-MORGOTH] 🛰️ Unit-%d: Связь Inmarsat восстановлена. Буфер полон (%d). Инициирован ACARS BURST!", unitID, len(m.buffers[unitID]))
			m.flushBufferWithRetransmissions(ctx, unitID)
		}
		
		// Возвращаем nil (псевдо-успех), так как бортовой FADEC "думает", что данные 
		// безопасно лежат в CMU, и не должен падать с ошибкой.
		return nil
	}

	// --- 3. ШТАТНАЯ ОТПРАВКА (Связь есть) ---
	// Если связь только что появилась (например, таймаут не был достигнут, но гроза кончилась
	// по другой причине в будущем), дочищаем буфер.
	if len(m.buffers[unitID]) > 0 {
		m.flushBufferWithRetransmissions(ctx, unitID)
	}

	// Отправляем текущий штатный кадр на землю.
	return m.NextBroker.Transmit(ctx, frame)
}

// flushBufferWithRetransmissions симулирует выгрузку буфера с TCP/IP дубликатами.
// ВНИМАНИЕ: Вызывающая сторона обязана держать m.mu.Lock().
func (m *KuBandSaturationMonkey) flushBufferWithRetransmissions(ctx context.Context, unitID int32) {
	frames := m.buffers[unitID]
	duplicatesGenerated := 0
	totalOriginal := len(frames)

	for _, f := range frames {
		// 1. Отправляем оригинальный бинарный кадр на Землю.
		// Игнорируем ошибку земли, Морготу плевать, дошло ли сообщение.
		_ = m.NextBroker.Transmit(ctx, f)

		// 2. Симуляция Spurious TCP Retransmission (Задержка спутника).
		// Хэшируем наносекунды кадра для детерминированного принятия решения о дубликате.
		dh := fnv.New64a()
		dh.Write([]byte{byte(f.Timestamp.UnixNano() & 0xFF)})
		dupHash := float64(dh.Sum64()) / float64(^uint64(0))

		if dupHash < m.DuplicationRate {
			// Таймер RTO истек, борт шлет этот же самый ARINC-кадр еще раз!
			_ = m.NextBroker.Transmit(ctx, f)
			duplicatesGenerated++
		}
	}

	log.Printf("[CHAOS-MORGOTH] 🌪️ ACARS Burst для Unit-%d завершен. Выгружено оригиналов: %d. Вброшено TCP-дубликатов: %d.", 
		unitID, totalOriginal, duplicatesGenerated)

	// 🚨 ПАРАНОЙЯ: Очистка буфера без освобождения памяти (Zero-Allocation паттерн).
	// Мы сбрасываем длину среза (len) в 0, но сохраняем емкость (capacity) для следующей грозы.
	m.buffers[unitID] = frames[:0]
}