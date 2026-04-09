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

// InitLogger перенаправляет стандартный вывод логов в файл.
// 🚨 ПАРАНОЙЯ L1 (TUI Tearing Protection): Гарантирует, что консоль останется чистой для pterm.
func InitLogger(logPath string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(logPath), 0755); err != nil {
		return nil, fmt.Errorf("ошибка создания директории логов: %w", err)
	}

	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("ошибка открытия файла логов: %w", err)
	}

	log.SetOutput(file)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	return file, nil
}

// StartDashboard запускает изолированную горутину для отрисовки TUI.
// AI-Ready: Read-Only доступ к SharedMetrics (без мьютексов).
func StartDashboard(ctx context.Context, m *flight.SharedMetrics, runID string, mode string, refreshRateMs int) {
	// Инициализируем зону отрисовки
	area, _ := pterm.DefaultArea.WithCenter().Start()
	defer area.Stop()

	ticker := time.NewTicker(time.Duration(refreshRateMs) * time.Millisecond)
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			// Финальная отрисовка
			finalMsg := pterm.DefaultBox.WithTitle("SHUTDOWN").Sprint(pterm.Yellow("Система останавливается. Дождитесь посадки бортов..."))
			area.Update(finalMsg)
			// Не вызываем area.Stop() здесь, чтобы сообщение осталось висеть, пока идет каскадное выключение.
			// area остановится сама, когда процесс завершится, или можно оставить defer area.Stop() 
			// но добавить небольшую задержку, хотя лучше убрать defer и не стирать.
			return
		case <-ticker.C:
			uptime := time.Since(startTime).Round(time.Second)
			
			// Атомарное чтение метрик (Zero Locks)
			active := m.ActivePilots.Load()
			flights := m.TotalFlights.Load()
			points := m.PointsGenerated.Load()
			files := m.FilesSaved.Load()
			errors := m.Errors.Load()

			// Цветовая кодировка ошибок (Здоровая паранойя)
			errorStr := pterm.Green("0")
			if errors > 0 {
				errorStr = pterm.Red(fmt.Sprintf("%d", errors))
			}

			modeStr := pterm.Green(mode)
			if mode == "ACCELERATED" {
				modeStr = pterm.Yellow(mode)
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
				pterm.Cyan("Mode:"), modeStr,
				pterm.Cyan("Uptime:"), uptime,
				pterm.Blue("Active Pilots:"), active,
				pterm.Blue("Total Flights:"), flights,
				pterm.Magenta("Telemetry Pts:"), points,
				pterm.Magenta("Files Saved:"), files,
				pterm.Red("Critical Errs:"), errorStr,
			)

			// Double Buffering — обновляем панель целиком без мерцания
			box := pterm.DefaultBox.WithTitle("✈️ C-MAPSS Telemetry Factory 4.0").Sprint(content)
			area.Update(box)
		}
	}
}