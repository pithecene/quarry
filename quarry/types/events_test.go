package types

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
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

// RawFrameBytes is runtime bookkeeping: neither encoding of an envelope
// carries it, so setting it changes neither encoding.
func TestEventEnvelope_RawFrameBytesStaysOffTheWire(t *testing.T) {
	t.Parallel()
	encodings := []struct {
		name    string
		marshal func(any) ([]byte, error)
	}{
		{"msgpack", msgpack.Marshal},
		{"json", json.Marshal},
	}
	for _, enc := range encodings {
		t.Run(enc.name, func(t *testing.T) {
			plain := EventEnvelope{EventID: "e1", Type: EventTypeItem, Seq: 1}
			sized := plain
			sized.RawFrameBytes = 12345
			a, errA := enc.marshal(plain)
			b, errB := enc.marshal(sized)
			if errA != nil || errB != nil {
				t.Fatalf("marshal: %v, %v", errA, errB)
			}
			if !bytes.Equal(a, b) {
				t.Errorf("%s encoding changed when RawFrameBytes was set:\n%q\n%q", enc.name, a, b)
			}
		})
	}
}
