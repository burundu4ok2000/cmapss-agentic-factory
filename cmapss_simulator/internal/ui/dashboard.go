package ui

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"cmapss_simulator/internal/flight"

	"github.com/pterm/pterm"
)

// ==============================================================================
// ENTERPRISE TELEMETRY DASHBOARD (Control Plane UI)
// AI-Ready Standard:
// 1. Строгое разделение потоков вывода: UI -> Stdout, Audit -> File.
// 2. Lock-Free чтение метрик.
// 3. Защита от TUI Tearing при экстренном завершении (Graceful UI Teardown).
// ==============================================================================

// InitLogger перенаправляет стандартный вывод системных логов в изолированный файл.
// 🚨 ПАРАНОЙЯ L1 (TUI Tearing Protection): В Enterprise-системах CLI-интерфейс
// (stdout) должен оставаться чистым для дашборда оператора, а системные логи (аудит)
// обязаны писаться в append-only файл для сборщиков логов (Filebeat/Promtail/Splunk).
func InitLogger(logPath string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(logPath), 0755); err != nil {
		return nil, fmt.Errorf("ошибка создания директории логов: %w", err)
	}

	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("ошибка открытия файла логов: %w", err)
	}

	// Жестко захватываем дефолтный логгер
	log.SetOutput(file)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	return file, nil
}

// StartDashboard запускает изолированную горутину для отрисовки TUI.
// AI-Ready: Использует Read-Only доступ к SharedMetrics (без мьютексов).
func StartDashboard(ctx context.Context, m *flight.SharedMetrics, runID string, mode string, refreshRateMs int) {
	// Инициализируем зону отрисовки (Area)
	area, err := pterm.DefaultArea.WithCenter().Start()
	if err != nil {
		log.Printf("CRITICAL: Ошибка запуска TUI: %v", err)
		return
	}

	// 🚨 ПАРАНОЙЯ L2 (Defensive Defer): Защита терминала на случай непредвиденной 
	// паники внутри самой горутины дашборда.
	defer area.Stop()

	ticker := time.NewTicker(time.Duration(refreshRateMs) * time.Millisecond)
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			// 🚨 ПАРАНОЙЯ L3 (Clean Exit & Anti-Tearing):
			// Получен сигнал на экстренную остановку Завода (Graceful Shutdown).
			
			// 1. Рисуем финальный кадр-предупреждение для Оператора.
			finalMsg := pterm.DefaultBox.WithTitle("SHUTDOWN SEQUENCE").
				Sprint(pterm.Yellow("Система переведена в режим изоляции. Дождитесь посадки бортов..."))
			area.Update(finalMsg)
			
			// 2. ЯВНО и немедленно останавливаем Area.
			// Это очищает виртуальный буфер pterm и возвращает контроль над терминалом
			// стандартному stdout. Без этого шага финальные логи main.go будут "зажеваны".
			area.Stop()
			
			// 3. Выводим визуальный разделитель, чтобы отбить TUI от системных логов выключения.
			fmt.Println("\n====================================================================")
			fmt.Println(" TUI ОСТАНОВЛЕН. ТЕРМИНАЛ ВОЗВРАЩЕН ДЛЯ ВЫВОДА ЛОГОВ ЗАВЕРШЕНИЯ ")
			fmt.Println("====================================================================")
			return

		case <-ticker.C:
			uptime := time.Since(startTime).Round(time.Second)
			
			// Атомарное чтение метрик (Zero Locks = Максимальная скорость)
			active := m.ActivePilots.Load()
			flights := m.TotalFlights.Load()
			points := m.PointsGenerated.Load()
			files := m.FilesSaved.Load()
			errors := m.Errors.Load()

			// Цветовая кодировка аномалий (Operator Risk Awareness)
			errorStr := pterm.Green("0")
			if errors > 0 {
				errorStr = pterm.Red(fmt.Sprintf("%d", errors))
			}

			content := fmt.Sprintf(`
 %s %s
 %s %s
 %s %s

 %s %d / 100
 %s %d
 %s %d
 %s %d
 %s %s
`,
				pterm.Cyan("Run ID:"), runID,
				pterm.Cyan("Mode:"), mode,
				pterm.Cyan("Uptime:"), uptime,
				pterm.Blue("Active Pilots:"), active,
				pterm.Blue("Total Flights:"), flights,
				pterm.Magenta("Telemetry Pts:"), points,
				pterm.Magenta("Files Saved:"), files,
				pterm.Red("Critical Errs:"), errorStr,
			)

			// Double Buffering — обновляем панель целиком без мерцания (Anti-Flicker)
			box := pterm.DefaultBox.WithTitle("✈️ C-MAPSS Telemetry Factory 4.0").Sprint(content)
			area.Update(box)
		}
	}
}