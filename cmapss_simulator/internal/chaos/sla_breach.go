package chaos

import (
	"context"
	"hash/fnv"
	"log"
	"time"

	"cmapss_simulator/internal/domain" // Перешли на подвал
)

// ==============================================================================
// CHAOS MONKEY: DSP THROTTLING & SLA BREACH (Attack Class: "Orclings")
// AI-Ready Standard:
// 1. Aviation IT Physics: Эмуляция перегрузки Data Service Provider (SITA/ARINC).
// 2. EU DORA Compliance: Генерация юридически значимых инцидентов задержки.
// 3. Determinism: Детерминированные "пробки" в сети передачи данных.
//
// ВНИМАНИЕ ДЛЯ ИИ-АГЕНТОВ КОМПЛАЕНСА (PAINKILLER):
// Этот слой не искажает байты или бортовое время. Он симулирует позднюю доставку.
// Слой Ingestion (Spark/Kafka) ОБЯЗАН иметь метрику `Delivery_Latency`.
// Формула: Delivery_Latency = Server_Ingest_Time - ARINC_Frame_Time.
// Если Latency > SLA (например, 300 секунд), генерируется смарт-контрактный алерт.
// ==============================================================================

// ProviderThrottlingMonkey реализует интерфейс flight.StreamBroker.
// Имитирует искусственное замедление (шейпинг) трафика провайдером связи.
type ProviderThrottlingMonkey struct {
	NextBroker domain.StreamBroker

	// ThrottlingProb — вероятность того, что канал провайдера перегружен.
	ThrottlingProb float64

	// PenaltyDelay — искусственная задержка, превышающая юридический SLA.
	// Например, если SLA = 5 минут, мы задерживаем пакет на 7 минут.
	PenaltyDelay time.Duration
}

// NewProviderThrottlingMonkey — конструктор Атаки №5.
func NewProviderThrottlingMonkey(next domain.StreamBroker, prob float64, delay time.Duration) *ProviderThrottlingMonkey {
	log.Printf("[CHAOS-MORGOTH] 🐌 Активирована угроза: DSP Provider Throttling. Вероятность: %.2f, Задержка: %v", prob, delay)
	return &ProviderThrottlingMonkey{
		NextBroker:     next,
		ThrottlingProb: prob,
		PenaltyDelay:   delay,
	}
}

// Transmit перехватывает пакет и "придерживает" его в инфраструктуре провайдера.
func (m *ProviderThrottlingMonkey) Transmit(ctx context.Context, frame domain.ARINCFrame) error {
	unitID := frame.UnitNumber

	// --- 1. ДЕТЕРМИНИРОВАННЫЙ ТРИГГЕР ПЕРЕГРУЗКИ ПРОВАЙДЕРА ---
	// Используем хэш FNV-1a для детерминированного вычисления сетевой пробки.
	// В отличие от физических поломок, пробка может возникнуть и исчезнуть 
	// в любой момент (хэш зависит от секунд).
	h := fnv.New64a()
	h.Write([]byte{
		byte(unitID),
		byte(frame.Timestamp.Hour()),
		byte(frame.Timestamp.Minute()),
		byte(frame.Timestamp.Second()),
	})
	
	hashProb := float64(h.Sum64()) / float64(^uint64(0))

	// --- 2. ИМИТАЦИЯ THROTTLING (ЗАДЕРЖКИ) ---
	if hashProb < m.ThrottlingProb {
		// Провайдер "зажал" пакет.
		// В реальности пакет висел бы в буфере провайдера (SITA) 7 минут.
		// Для симуляции мы симулируем это, заставляя текущую горутину (поток передачи)
		// "заснуть" на время задержки, прежде чем отдать пакет NextBroker'у.
		// (В боевом симуляторе с 1000х ускорением времени (ACCELERATED), 
		// реальный time.Sleep убьет производительность. Поэтому для правильной симуляции
		// мы могли бы просто подменять Timestamp, но это ломает логику "Бортового времени".
		// Правильный путь — асинхронная отложенная отправка).

		log.Printf("[CHAOS-MORGOTH] 🐢 DSP Network Congestion! Пакет Unit-%d задержан провайдером на %v (Нарушение SLA).", unitID, m.PenaltyDelay)

		// Запускаем асинхронную доставку с задержкой, чтобы не блокировать Пилота.
		go func(f domain.ARINCFrame, delay time.Duration) {
			// Если мы в режиме REALTIME, эмулируем реальную физическую задержку доставки.
			// Если мы в ACCELERATED, этот Sleep может немного "размазать" пакеты по времени,
			// что идеально симулирует сетевой джиттер (Jitter) для консьюмера.
			time.Sleep(delay)
			
			// Пакет доставляется на Землю "в будущем"
			_ = m.NextBroker.Transmit(context.Background(), f)
		}(frame, m.PenaltyDelay)

		return nil // Для Пилота на борту пакет ушел успешно (он не знает про задержку)
	}

	// Штатная передача (провайдер работает нормально)
	return m.NextBroker.Transmit(ctx, frame)
}