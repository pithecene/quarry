package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/pithecene/quarry/cli/reader"
)

// StatsModel is a Bubble Tea model for stats views.
type StatsModel struct {
	viewType string
	data     any
	width    int
	height   int
	quitting bool
}

// NewStatsModel creates a new stats model.
func NewStatsModel(viewType string, data any) StatsModel {
	return StatsModel{
		viewType: viewType,
		data:     data,
	}
}

// Init implements tea.Model.
func (m StatsModel) Init() tea.Cmd {
	return nil
}

// Update implements tea.Model.
func (m StatsModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tea.KeyMsg:
		if key.Matches(msg, keys.Quit) {
			m.quitting = true
			return m, tea.Quit
		}
	}

	return m, nil
}

// View implements tea.Model.
func (m StatsModel) View() string {
	if m.quitting {
		return ""
	}

	var content string
	switch m.viewType {
	case "stats_runs":
		content = m.renderStatsRuns()
	case "stats_jobs":
		content = m.renderStatsJobs()
	case "stats_tasks":
		content = m.renderStatsTasks()
	case "stats_proxies":
		content = m.renderStatsProxies()
	case "stats_executors":
		content = m.renderStatsExecutors()
	default:
		content = fmt.Sprintf("Unknown view type: %s", m.viewType)
	}

	help := HelpStyle.Render("Press q or Ctrl+C to quit")
	return content + "\n" + help
}

func (m StatsModel) renderStatsRuns() string {
	data, ok := m.data.(*reader.RunStats)
	if !ok {
		return "Invalid data type for stats_runs"
	}

	var b strings.Builder
	b.WriteString(TitleStyle.Render("Run Statistics"))
	b.WriteString("\n\n")

	// Create stat boxes
	boxes := []string{
		m.renderStatBox("Total", data.Total, lipgloss.Color("#3B82F6")),
		m.renderStatBox("Running", data.Running, warningColor),
		m.renderStatBox("Succeeded", data.Succeeded, successColor),
		m.renderStatBox("Failed", data.Failed, errorColor),
	}

	// Join boxes horizontally
	b.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, boxes...))

	return b.String()
}

func (m StatsModel) renderStatsJobs() string {
	data, ok := m.data.(*reader.JobStats)
	if !ok {
		return "Invalid data type for stats_jobs"
	}

	var b strings.Builder
	b.WriteString(TitleStyle.Render("Job Statistics"))
	b.WriteString("\n\n")

	boxes := []string{
		m.renderStatBox("Total", data.Total, lipgloss.Color("#3B82F6")),
		m.renderStatBox("Running", data.Running, warningColor),
		m.renderStatBox("Succeeded", data.Succeeded, successColor),
		m.renderStatBox("Failed", data.Failed, errorColor),
	}

	b.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, boxes...))

	return b.String()
}

func (m StatsModel) renderStatsTasks() string {
	data, ok := m.data.(*reader.TaskStats)
	if !ok {
		return "Invalid data type for stats_tasks"
	}

	var b strings.Builder
	b.WriteString(TitleStyle.Render("Task Statistics"))
	b.WriteString("\n\n")

	boxes := []string{
		m.renderStatBox("Total", data.Total, lipgloss.Color("#3B82F6")),
		m.renderStatBox("Running", data.Running, warningColor),
		m.renderStatBox("Succeeded", data.Succeeded, successColor),
		m.renderStatBox("Failed", data.Failed, errorColor),
	}

	b.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, boxes...))

	return b.String()
}

func (m StatsModel) renderStatsProxies() string {
	data, ok := m.data.([]reader.ProxyStats)
	if !ok {
		return "Invalid data type for stats_proxies"
	}

	var b strings.Builder
	b.WriteString(TitleStyle.Render("Proxy Statistics"))
	b.WriteString("\n\n")

	for i, proxy := range data {
		if i > 0 {
			b.WriteString("\n\n")
		}

		poolTitle := lipgloss.NewStyle().
			Bold(true).
			Foreground(highlightColor).
			Render(fmt.Sprintf("Pool: %s", proxy.Pool))

		b.WriteString(poolTitle)
		b.WriteString("\n")

		boxes := []string{
			m.renderStatBox("Requests", proxy.Requests, lipgloss.Color("#3B82F6")),
			m.renderStatBox("Failures", proxy.Failures, errorColor),
		}

		b.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, boxes...))

		if proxy.LastUsedAt != nil {
			b.WriteString("\n")
			b.WriteString(fmt.Sprintf("%s %s",
				LabelStyle.Render("Last Used:"),
				ValueStyle.Render(proxy.LastUsedAt.Format("2006-01-02 15:04:05"))))
		}
	}

	return b.String()
}

func (m StatsModel) renderStatsExecutors() string {
	data, ok := m.data.(*reader.ExecutorStats)
	if !ok {
		return "Invalid data type for stats_executors"
	}

	var b strings.Builder
	b.WriteString(TitleStyle.Render("Executor Statistics"))
	b.WriteString("\n\n")

	boxes := []string{
		m.renderStatBox("Total", data.Total, lipgloss.Color("#3B82F6")),
		m.renderStatBox("Running", data.Running, warningColor),
		m.renderStatBox("Idle", data.Idle, successColor),
		m.renderStatBox("Failed", data.Failed, errorColor),
	}

	b.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, boxes...))

	return b.String()
}

func (m StatsModel) renderStatBox(label string, value int, color lipgloss.Color) string {
	boxStyle := StatBoxStyle.BorderForeground(color)

	valueStr := StatValueStyle.Foreground(color).Render(fmt.Sprintf("%d", value))
	labelStr := StatLabelStyle.Render(label)

	content := lipgloss.JoinVertical(lipgloss.Center, valueStr, labelStr)

	return boxStyle.Render(content)
}

// RunStatsTUI runs the stats TUI.
func RunStatsTUI(viewType string, data any) error {
	model := NewStatsModel(viewType, data)
	p := tea.NewProgram(model, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

// RenderStatsStatic renders stats data without full TUI (for fallback).
func RenderStatsStatic(viewType string, data any) string {
	model := NewStatsModel(viewType, data)
	model.width = 80
	model.height = 24
	return lipgloss.NewStyle().Padding(1, 2).Render(model.View())
}
