package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/pithecene/quarry/cli/reader"
)

// InspectModel is a Bubble Tea model for inspect views.
type InspectModel struct {
	viewType string
	data     any
	width    int
	height   int
	quitting bool
}

// NewInspectModel creates a new inspect model.
func NewInspectModel(viewType string, data any) InspectModel {
	return InspectModel{
		viewType: viewType,
		data:     data,
	}
}

// Init implements tea.Model.
func (m InspectModel) Init() tea.Cmd {
	return nil
}

// Update implements tea.Model.
func (m InspectModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
func (m InspectModel) View() string {
	if m.quitting {
		return ""
	}

	var content string
	switch m.viewType {
	case "inspect_run":
		content = m.renderInspectRun()
	case "inspect_proxy":
		content = m.renderInspectProxy()
	default:
		content = fmt.Sprintf("Unknown view type: %s", m.viewType)
	}

	help := HelpStyle.Render("Press q or Ctrl+C to quit")
	return content + "\n" + help
}

func (m InspectModel) renderInspectRun() string {
	data, ok := m.data.(*reader.InspectRunResponse)
	if !ok {
		return "Invalid data type for inspect_run"
	}

	var b strings.Builder
	b.WriteString(TitleStyle.Render("Run Details"))
	b.WriteString("\n\n")

	rows := [][]string{
		{"Run ID", data.RunID},
		{"Job ID", data.JobID},
		{"State", data.State},
		{"Attempt", fmt.Sprintf("%d", data.Attempt)},
		{"Policy", data.Policy},
		{"Started At", data.StartedAt.Format("2006-01-02 15:04:05")},
	}

	if data.ParentRun != nil {
		rows = append(rows, []string{"Parent Run", *data.ParentRun})
	}

	if data.EndedAt != nil {
		rows = append(rows, []string{"Ended At", data.EndedAt.Format("2006-01-02 15:04:05")})
	}

	for _, row := range rows {
		label := LabelStyle.Render(row[0] + ":")
		value := row[1]
		if row[0] == "State" {
			value = StateStyle(data.State).Render(value)
		} else {
			value = ValueStyle.Render(value)
		}
		fmt.Fprintf(&b, "%s %s\n", label, value)
	}

	return BoxStyle.Render(b.String())
}

func (m InspectModel) renderInspectProxy() string {
	data, ok := m.data.(*reader.InspectProxyPoolResponse)
	if !ok {
		return "Invalid data type for inspect_proxy"
	}

	var b strings.Builder
	b.WriteString(TitleStyle.Render("Proxy Pool Details"))
	b.WriteString("\n\n")

	fmt.Fprintf(&b, "%s %s\n",
		LabelStyle.Render("Name:"),
		ValueStyle.Render(data.Name))
	fmt.Fprintf(&b, "%s %s\n",
		LabelStyle.Render("Strategy:"),
		ValueStyle.Render(data.Strategy))
	fmt.Fprintf(&b, "%s %s\n",
		LabelStyle.Render("Endpoints:"),
		ValueStyle.Render(fmt.Sprintf("%d", data.EndpointCnt)))

	if data.Sticky != nil {
		b.WriteString("\n")
		b.WriteString(TitleStyle.Render("Sticky Config"))
		b.WriteString("\n")
		fmt.Fprintf(&b, "%s %s\n",
			LabelStyle.Render("  Scope:"),
			ValueStyle.Render(data.Sticky.Scope))
		if data.Sticky.TTLMs != nil {
			fmt.Fprintf(&b, "%s %s\n",
				LabelStyle.Render("  TTL:"),
				ValueStyle.Render(fmt.Sprintf("%dms", *data.Sticky.TTLMs)))
		}
	}

	b.WriteString("\n")
	b.WriteString(TitleStyle.Render("Runtime State"))
	b.WriteString("\n")
	fmt.Fprintf(&b, "%s %s\n",
		LabelStyle.Render("  RR Index:"),
		ValueStyle.Render(fmt.Sprintf("%d", data.Runtime.RoundRobinIndex)))
	fmt.Fprintf(&b, "%s %s\n",
		LabelStyle.Render("  Sticky:"),
		ValueStyle.Render(fmt.Sprintf("%d entries", data.Runtime.StickyEntries)))
	if data.Runtime.LastUsedAt != nil {
		fmt.Fprintf(&b, "%s %s\n",
			LabelStyle.Render("  Last Used:"),
			ValueStyle.Render(data.Runtime.LastUsedAt.Format("2006-01-02 15:04:05")))
	}

	return BoxStyle.Render(b.String())
}

// keyMap defines key bindings.
type keyMap struct {
	Quit key.Binding
}

var keys = keyMap{
	Quit: key.NewBinding(
		key.WithKeys("q", "ctrl+c"),
		key.WithHelp("q", "quit"),
	),
}

// RunInspectTUI runs the inspect TUI.
func RunInspectTUI(viewType string, data any) error {
	model := NewInspectModel(viewType, data)
	p := tea.NewProgram(model, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

// RenderInspectStatic renders inspect data without full TUI (for fallback).
func RenderInspectStatic(viewType string, data any) string {
	model := NewInspectModel(viewType, data)
	model.width = 80
	model.height = 24
	return lipgloss.NewStyle().Padding(1, 2).Render(model.View())
}
