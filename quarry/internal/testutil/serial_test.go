package testutil_test

import (
	"testing"

	"github.com/pithecene/quarry/internal/testutil"
)

func TestSerial_AcceptsAReason(t *testing.T) {
	testutil.Serial(t, "exercises the serial declaration itself")
}
