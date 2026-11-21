package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/pithecene/quarry/cli/reader"
)

// InspectModel is a Bubble Tea model for inspect views.
type InspectModel struct {
	viewType    string
	data        any
	quitting    bool
	runViewport viewport.Model
	runFrame    lipgloss.Style
	runHelp     string
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
		if m.viewType == "inspect_run" {
			m.resizeRunViewport(msg.Width, msg.Height)
		}
		return m, nil

	case tea.KeyMsg:
		if key.Matches(msg, keys.Quit) {
			m.quitting = true
			return m, tea.Quit
		}
	}

	if m.runViewport.Width > 0 {
		var cmd tea.Cmd
		m.runViewport, cmd = m.runViewport.Update(msg)
		return m, cmd
	}
	return m, nil
}

const inspectRunHelp = "Arrow keys scroll. Press q or Ctrl+C to quit."

func (m *InspectModel) resizeRunViewport(width, height int) {
	m.runHelp = HelpStyle.MaxWidth(max(1, width)).Render(inspectRunHelp)
	available := height - lipgloss.Height(m.runHelp)
	m.runFrame = BoxStyle
	if width <= BoxStyle.GetHorizontalFrameSize() || available <= BoxStyle.GetVerticalFrameSize() {
		m.runFrame = lipgloss.NewStyle()
	}
	if m.runViewport.Width == 0 {
		m.runViewport = viewport.New(0, 0)
		m.runViewport.SetHorizontalStep(8)
		m.runViewport.SetContent(strings.TrimSuffix(m.renderInspectRun(), "\n"))
	}
	m.runViewport.Width = max(1, width-m.runFrame.GetHorizontalFrameSize())
	m.runViewport.Height = min(m.runViewport.TotalLineCount(), max(1, available-m.runFrame.GetVerticalFrameSize()))
	m.runViewport.SetYOffset(m.runViewport.YOffset)
	m.runViewport.ScrollRight(0) // Clamp the horizontal offset after a resize.
}

// View implements tea.Model.
func (m InspectModel) View() string {
	if m.quitting {
		return ""
	}

	var content string
	switch m.viewType {
	case "inspect_run":
		if m.runViewport.Width > 0 {
			return m.runFrame.Render(m.runViewport.View()) + "\n" + m.runHelp
		}
		content = BoxStyle.Render(m.renderInspectRun())
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

	run := data.Run
	rows := [][]string{
		{"Run ID", run.RunID},
		{"Job ID", orDash(run.JobID)},
		{"State", string(run.State)},
		{"Outcome", orDash(run.Outcome)},
		{"Attempt", fmt.Sprintf("%d", run.Attempt)},
		{"Policy", orDash(data.Policy)},
		{"Started At", orDash(run.StartedAt)},
		{"Deadline At", orDash(run.DeadlineAt)},
		{"Ended At", orDash(run.EndedAt)},
		{"Terminal", orDash(run.Terminal)},
		{"Event Count", countOrDash(run.EventCount)},
		{"Message", orDash(run.Message)},
		{"Backfilled", fmt.Sprintf("%t", run.Backfilled)},
		{"Dataset", run.Dataset},
		{"Source", run.Source},
		{"Category", run.Category},
		{"Day", run.Day},
		{"Partition", run.Partition},
	}

	if run.ParentRunID != nil {
		rows = append(rows, []string{"Parent Run", *run.ParentRunID})
	}

	for _, row := range rows {
		label := LabelStyle.Render(row[0] + ":")
		value := row[1]
		if row[0] == "State" {
			value = StateStyle(string(run.State)).Render(value)
		} else {
			value = ValueStyle.Render(value)
		}
		fmt.Fprintf(&b, "%s %s\n", label, value)
	}

	if len(data.SidecarFiles) > 0 {
		b.WriteString("\n")
		b.WriteString(TitleStyle.Render("Sidecar Files"))
		b.WriteString("\n")
	}
	for _, file := range data.SidecarFiles {
		fmt.Fprintf(&b, "%s (%s, %d bytes)\n  %s\n", file.Filename, file.ContentType, file.Size, file.Path)
	}

	return b.String()
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
	return lipgloss.NewStyle().Padding(1, 2).Render(model.View())
}

// orDash renders a field that a run's records may leave null.
func orDash(s *string) string {
	if s == nil {
		return "—"
	}
	return *s
}

// countOrDash renders a count, or a dash when the run recorded none.
func countOrDash(n *int64) string {
	if n == nil {
		return "—"
	}
	return fmt.Sprintf("%d", *n)
}
