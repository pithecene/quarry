package types //nolint:revive // types is a valid package name

import (
	"regexp"
	"testing"
)

func TestVersion_Format(t *testing.T) {
	t.Parallel()

	// Version should be a valid semver
	semverRegex := regexp.MustCompile(`^\d+\.\d+\.\d+(-[a-zA-Z0-9.]+)?$`)
	if !semverRegex.MatchString(Version) {
		t.Errorf("Version %q is not a valid semver", Version)
	}
}

func TestContractVersion_MatchesVersion(t *testing.T) {
	t.Parallel()

	// Per lockstep versioning, ContractVersion must equal Version
	if ContractVersion != Version {
		t.Errorf("ContractVersion %q != Version %q (lockstep versioning violated)", ContractVersion, Version)
	}
}
