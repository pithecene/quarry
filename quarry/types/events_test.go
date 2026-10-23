package types //nolint:revive // types is a valid package name

import (
	"testing"
)

func TestEventType_IsTerminal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		eventType EventType
		want      bool
	}{
		{EventTypeRunComplete, true},
		{EventTypeRunError, true},
		{EventTypeItem, false},
		{EventTypeArtifact, false},
		{EventTypeCheckpoint, false},
		{EventTypeLog, false},
		{EventTypeEnqueue, false},
		{EventTypeRotateProxy, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.eventType), func(t *testing.T) {
			got := tt.eventType.IsTerminal()
			if got != tt.want {
				t.Errorf("EventType(%q).IsTerminal() = %v, want %v", tt.eventType, got, tt.want)
			}
		})
	}
}
