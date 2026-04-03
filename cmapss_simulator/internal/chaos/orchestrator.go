package chaos

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"time"

	"cmapss_simulator/internal/database"
	"cmapss_simulator/internal/domain" // Перешли на подвал
	"cmapss_simulator/internal/physics"
)

// ==============================================================================
// THE CHAOS ORCHESTRATOR (Morgoth's Forge)
// AI-Ready Standard:
// 1. Dependency Injection: Сборка паттернов "Декоратор" и "Компоновщик".
// 2. Custom Profiles: Парсинг CSV-строки для активации любых комбинаций атак.
// 3. Centralized Toggling: Нулевое влияние на Валинор, если хаос выключен.
// ==============================================================================

// CompositeInjector позволяет применять несколько физических атак последовательно.
type CompositeInjector struct {
	injectors []domain.ChaosInjector
}

func (c *CompositeInjector) CorruptPhysicalRow(tick int, record *database.FlightRecord) {
	for _, inj := range c.injectors {
		inj.CorruptPhysicalRow(tick, record)
	}
}

// ChaosAssembler хранит собранные зависимости для передачи в main.go.
type ChaosAssembler struct {
	PhysicalInjector domain.ChaosInjector  // Передается в engine.go
	SatcomBroker     domain.StreamBroker    // Передается в worker.go
	QarInterceptor   func(ctx context.Context, rec database.FlightRecord, startTime time.Time, durationSec int, runID string, status string, cfg *physics.SimulatorConfig, saveFunc func(rec database.FlightRecord, startTime time.Time, durationSec int, runID string, status string, cfg physics.SimulatorConfig) error) error // Передается в worker.go
	LandingInterceptor func(context.Context, database.LandingReport, func(context.Context, []database.LandingReport)) // Передается в dispatcher.go
}

// BuildChaosRealm читает строку профиля и кует нужных Обезьян.
func BuildChaosRealm(profile string, baseBroker domain.StreamBroker, db *sql.DB) *ChaosAssembler {
	assembler := &ChaosAssembler{
		SatcomBroker: baseBroker,
	}

	if profile == "" || profile == "NONE" {
		log.Println("[CHAOS-ORCHESTRATOR] 🛡️ Профиль NONE. Завод работает в идеальном режиме (Валинор).")
		return assembler
	}

	log.Printf("[CHAOS-ORCHESTRATOR] 🔥 Активация хаоса. Профиль: %s", profile)

	attacks := strings.Split(profile, ",")
	var physicalInjectors []domain.ChaosInjector

	for _, attack := range attacks {
		attack = strings.TrimSpace(attack)
		
		switch attack {
		// --- ЭШЕЛОН 1: ОРКИ ---
		case "schema_rot":
			// Вероятность 0.005%, атакуем случайные борты (0)
			physicalInjectors = append(physicalInjectors, NewArinc429BitFlipInjector(0.00005, 0))
		
		case "network_burst":
			// Вероятность шторма 1%, 15% дубликатов, буфер 180 секунд
			assembler.SatcomBroker = NewKuBandSaturationMonkey(assembler.SatcomBroker, 0.01, 0.15, 180)
		
		case "time_drift":
			// Вероятность 0.5% поймать WNRO
			assembler.SatcomBroker = NewGpsRolloverMonkey(assembler.SatcomBroker, 0.005)
		
		case "out_of_bounds":
			// Обрыв термопары T50 с микровероятностью
			physicalInjectors = append(physicalInjectors, NewThermocoupleFailureMonkey(0.00002, 0))
			
		case "sla_breach":
			// 5% шанс, что провайдер задержит пакет на 7 минут
			assembler.SatcomBroker = NewProviderThrottlingMonkey(assembler.SatcomBroker, 0.05, 7*time.Minute)

		// --- ЭШЕЛОН 2: БАЛРОГИ ---
		case "sensor_freeze":
			// Обледенение трубки Пито P30
			physicalInjectors = append(physicalInjectors, NewSensorFreezeMonkey(0.00003, 0))
		
		case "zstd_bomb":
			// 10% шанс, что Gatelink сломан, борт начнет копить данные и переключится на Lvl 22
			monkey := NewQarStorageMonkey(0.10)
			assembler.QarInterceptor = monkey.InterceptSave

		case "ocean_blackout":
			// ⚠️ ВАЖНО: Эта атака должна оборачивать сам Dispatcher.
			// Поскольку мы здесь возвращаем только Assembler, логика интеграции этой 
			// обезьяны потребует модификации NewDispatcher в main.go. 
			// Для простоты текущей архитектуры, мы пока оставим логирование.
			log.Println("[CHAOS-ORCHESTRATOR] 🌊 ocean_blackout требует обертки Dispatcher. Активируйте в main.go.")

		// --- ЭШЕЛОН 3: САУРОН ---
		case "adversarial_drift":
			// Внедрение APT. 2% шанс заражения, дрейф 0.00005 в секунду
			physicalInjectors = append(physicalInjectors, NewAdversarialDriftMonkey(0.02, 0.00005, 0))

		case "zombie_state":
			// 15% шанс, что механик забудет оформить документы на сгоревший борт
			monkey := NewZombieStateMonkey(db, 0.15)
			assembler.LandingInterceptor = monkey.InterceptLanding

		// --- ПРЕСЕТЫ (Алиасы) ---
		case "ORCS":
			return BuildChaosRealm("schema_rot,network_burst,time_drift,out_of_bounds,sla_breach", baseBroker, db)
		case "BALROGS":
			return BuildChaosRealm("sensor_freeze,zstd_bomb,ocean_blackout", baseBroker, db)
		case "SAURON":
			return BuildChaosRealm("adversarial_drift,zombie_state", baseBroker, db)
		case "APOCALYPSE":
			return BuildChaosRealm("schema_rot,network_burst,time_drift,out_of_bounds,sla_breach,sensor_freeze,zstd_bomb,ocean_blackout,adversarial_drift,zombie_state", baseBroker, db)
			
		default:
			log.Printf("[CHAOS-ORCHESTRATOR] ❓ Неизвестная угроза игнорируется: %s", attack)
		}
	}

	if len(physicalInjectors) > 0 {
		assembler.PhysicalInjector = &CompositeInjector{injectors: physicalInjectors}
	}

	return assembler
}