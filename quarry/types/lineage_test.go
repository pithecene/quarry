package types //nolint:revive // types is a valid package name

import (
	"testing"
)

func TestRunMeta_Validate(t *testing.T) {
	t.Parallel()

	parent := "run-parent-001"

	tests := []struct {
		name    string
		meta    RunMeta
		wantErr bool
	}{
		{
			name:    "empty run_id",
			meta:    RunMeta{RunID: "", Attempt: 1},
			wantErr: true,
		},
		{
			name:    "attempt zero",
			meta:    RunMeta{RunID: "run-001", Attempt: 0},
			wantErr: true,
		},
		{
			name:    "initial run with parent_run_id",
			meta:    RunMeta{RunID: "run-001", Attempt: 1, ParentRunID: &parent},
			wantErr: true,
		},
		{
			name:    "retry run without parent_run_id",
			meta:    RunMeta{RunID: "run-001", Attempt: 2, ParentRunID: nil},
			wantErr: true,
		},
		{
			name:    "valid initial run",
			meta:    RunMeta{RunID: "run-001", Attempt: 1},
			wantErr: false,
		},
		{
			name:    "valid retry run",
			meta:    RunMeta{RunID: "run-002", Attempt: 2, ParentRunID: &parent},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.meta.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
