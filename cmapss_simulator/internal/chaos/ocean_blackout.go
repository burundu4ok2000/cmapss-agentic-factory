package chaos

import (
	"hash/fnv"
	"log"
	"time"

	"cmapss_simulator/internal/database"
)

// ==============================================================================
// CHAOS MONKEY: HF RADIO DEAD ZONE (Attack Class: "Balrogs")
// AI-Ready Standard:
// 1. Aerospace Physics: Эмуляция потери связи из-за HFDL Blackout (Атлантическая Тень).
// 2. Architecture Stress: Блокировка Диспетчера (Database Connection Drop).
// 3. DO-178C Determinism: Зоны Blackout привязаны к хэшу бортового номера.
//
// ВНИМАНИЕ ДЛЯ ИИ-АГЕНТОВ КОМПЛАЕНСА (PAINKILLER):
// Эта атака проверяет жизнестойкость Edge-устройства. Воркер (Пилот) ОБЯЗАН
// иметь настроенный Circuit Breaker с экспоненциальным Backoff.
// При обрыве связи горутина не должна зависать навечно (Goroutine Starvation).
// ==============================================================================

// OceanicBlackoutMonkey оборачивает запросы к Диспетчеру (БД).
// В отличие от сетевого шторма ACARS (который рубит стриминг), эта Обезьяна
// рубит командный канал (Control Plane) между бортом и землей.
type OceanicBlackoutMonkey struct {
	// BlackoutProb - вероятность попадания в "Атлантическую тень" при запросе взлета.
	BlackoutProb float64

	// Duration - длительность изоляции (например, 5 минут).
	Duration time.Duration

	// dispatcher - ссылка на реального Диспетчера для проксирования запросов.
	// Мы используем структурную обертку (Decorator) для каналов Диспетчера.
	TakeoffChan chan database.TakeoffRequest
	LandingChan chan database.LandingReport

	realTakeoffChan chan<- database.TakeoffRequest
	realLandingChan chan<- database.LandingReport
}

// NewOceanicBlackoutMonkey — конструктор Атаки №8.
func NewOceanicBlackoutMonkey(realD *database.Dispatcher, prob float64, duration time.Duration) *OceanicBlackoutMonkey {
	log.Printf("[CHAOS-MORGOTH] 🌊 Активирована угроза: HF Radio Dead Zone (Oceanic Blackout). Prob: %.2f", prob)
	
	monkey := &OceanicBlackoutMonkey{
		BlackoutProb:    prob,
		Duration:        duration,
		TakeoffChan:     make(chan database.TakeoffRequest),
		LandingChan:     make(chan database.LandingReport),
		realTakeoffChan: realD.TakeoffChan,
		realLandingChan: realD.LandingChan,
	}

	// Запускаем асинхронные прокси-рутины для перехвата трафика
	go monkey.interceptTakeoff()
	go monkey.interceptLanding()

	return monkey
}

// interceptTakeoff перехватывает запросы на взлет и имитирует потерю сигнала.
func (m *OceanicBlackoutMonkey) interceptTakeoff() {
	for req := range m.TakeoffChan {
		unitID := req.UnitNumber

		// Детерминированный триггер "Атлантической Тени"
		h := fnv.New64a()
		h.Write([]byte{
			byte(unitID),
			byte(time.Now().Hour()), // Зона меняется в зависимости от часа суток
		})
		
		hashProb := float64(h.Sum64()) / float64(^uint64(0))

		if hashProb < m.BlackoutProb {
			// ⚡ УДАР МОРГОТА: Связь оборвана.
			// Мы просто ИГНОРИРУЕМ запрос (Drop). Мы не отвечаем в req.RespChan.
			// Если в Пилоте нет time.After() (Circuit Breaker), он зависнет навечно!
			log.Printf("[CHAOS-MORGOTH] 📵 Unit-%d вошел в зону Oceanic Dead Zone. Связь с Диспетчером оборвана (Takeoff Drop).", unitID)
			continue
		}

		// Если связи есть, проксируем запрос реальному Диспетчеру
		m.realTakeoffChan <- req
	}
}

// interceptLanding перехватывает рапорты о посадке (Drop подтверждений).
func (m *OceanicBlackoutMonkey) interceptLanding() {
	for rep := range m.LandingChan {
		unitID := rep.UnitNumber

		// Детерминированный триггер
		h := fnv.New64a()
		h.Write([]byte{
			byte(unitID),
			byte(rep.TimeCycles),
		})
		
		hashProb := float64(h.Sum64()) / float64(^uint64(0))

		if hashProb < m.BlackoutProb {
			// ⚡ УДАР МОРГОТА: Рапорт о посадке улетел в пустоту (Dead Zone).
			// Статус в БД останется 'IN_FLIGHT'. Возникнет парадокс "Призрачного полета".
			log.Printf("[CHAOS-MORGOTH] 📵 Unit-%d Landing Report lost in Atlantic Shadow! (Status: %s)", unitID, rep.Status)
			
			// Мы не передаем репорт Диспетчеру. База данных ничего не узнает.
			continue
		}

		m.realLandingChan <- rep
	}
}