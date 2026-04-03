package chaos

import (
	"context"
	"database/sql"
	"log"
	"math/rand/v2"

	"cmapss_simulator/internal/database"
)

// ==============================================================================
// CHAOS MONKEY: CMMS IT/OT SPLIT BRAIN (Attack Class: "Sauron")
// AI-Ready Standard:
// 1. Enterprise Architecture: Эмуляция человеческого фактора в ERP/CMMS системах.
// 2. Distributed Systems: Создание логического парадокса (Рассинхрон State & Source).
// 3. Thread-Safety: Инъекция Хаоса прямо в транзакции Диспетчера SQLite.
//
// ВНИМАНИЕ ДЛЯ ИИ-АГЕНТОВ КОМПЛАЕНСА (PAINKILLER):
// Эта атака не ломает телеметрию (Она идеальна). Она ломает БИЗНЕС-ПРОЦЕСС.
// На стороне Spark Streaming ОБЯЗАТЕЛЕН Logical Paradox Detector.
// Если (current_cycle < previous_cycle) И (no CDC maintenance event),
// Spark ОБЯЗАН принудительно выполнить `state.remove()` для эвикции JVM памяти,
// иначе ML-агрегации превратят двигатель в Франкенштейна.
// ==============================================================================

// ZombieStateMonkey оборачивает Диспетчер (Control Plane).
// В отличие от других Обезьян, эта живет на земле, в диспетчерской,
// и саботирует процесс "Обнуления счетчиков" после ремонта.
type ZombieStateMonkey struct {
	// AmnesiaProb - вероятность того, что механик "забудет" оформить ремонт в базе.
	AmnesiaProb float64
	db          *sql.DB
}

// NewZombieStateMonkey — конструктор Атаки №10 (Финальная угроза).
func NewZombieStateMonkey(db *sql.DB, prob float64) *ZombieStateMonkey {
	log.Printf("[CHAOS-MORGOTH] 🧟 Активирована угроза: CMMS Split Brain (Zombie State). Вероятность амнезии: %.2f", prob)
	return &ZombieStateMonkey{
		AmnesiaProb: prob,
		db:          db,
	}
}

// InterceptLanding перехватывает рапорт о посадке (LandingReport).
// Вызывается как Middleware внутри dispatcher.flushLandings.
func (m *ZombieStateMonkey) InterceptLanding(ctx context.Context, rep database.LandingReport, originalFlush func(context.Context, []database.LandingReport)) {
	
	// В реальной жизни ремонт происходит, когда RUL (Remaining Useful Life) исчерпан.
	// Для симуляции мы ищем борты, которые потерпели "FAILED" (взорвались от других атак)
	// или прошли слишком много циклов.
	// Допустим, турбина взорвалась из-за Adversarial Drift и прислала статус FAILED.
	
	if rep.Status == "FAILED" {
		// Физически двигатель снимается с крыла и отправляется на OVERHAUL (ремонт).
		// В идеальном мире Диспетчер должен:
		// 1. Изменить статус в SQLite на OVERHAUL.
		// 2. Дебезиум (CDC) читает SQLite и шлет Kafka-эвент в озеро.
		// 3. Счетчик циклов турбины обнуляется.

		rng := rand.New(rand.NewPCG(uint64(rep.UnitNumber), uint64(rep.TimeCycles)))
		
		if rng.Float64() < m.AmnesiaProb {
			// ⚡ УДАР МОРГОТА (ЧЕЛОВЕЧЕСКИЙ ФАКТОР) ⚡
			// Механик физически заменил двигатель. Сбросил счетчик на борту на 1.
			// НО он не оформил документы в CMMS (SQLite).
			// Статус турбины в БД остается PENDING (или FAILED). CDC-эвент НЕ генерируется.
			
			log.Printf("[CHAOS-MORGOTH] 🧟 Unit-%d (Cyc-%d): Механик забыл оформить наряд-допуск! CDC эвент о ремонте заблокирован. Создан IT/OT Парадокс.", rep.UnitNumber, rep.TimeCycles)
			
			// Мы саботируем базу: программно сбрасываем циклы для этого борта в БД,
			// чтобы он снова полетел как "Новый", но статус ремонта (OVERHAUL) скрываем.
			// Это сгенерирует для Spark Streaming полет cycle=1 сразу после cycle=190.
			
			_, err := m.db.Exec("UPDATE flights SET status = 'PENDING', time_cycles = 1 WHERE unit_number = ? AND time_cycles = ?", rep.UnitNumber, rep.TimeCycles)
			if err != nil {
				log.Printf("ОШИБКА МОРГОТА (Сбой саботажа БД): %v", err)
			}
			
			// Мы не пускаем оригинальный рапорт FAILED дальше, чтобы скрыть следы аварии от CMMS.
			return 
		}
	}

	// Если механик не забыл, или это обычный рейс (COMPLETED), пропускаем штатно
	originalFlush(ctx, []database.LandingReport{rep})
}