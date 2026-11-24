package cmd

import (
	"time"

	"github.com/urfave/cli/v2"

	quarryconfig "github.com/pithecene/quarry/cli/config"
)

// resolveString returns the CLI flag value if explicitly set, else the config
// value if non-empty, else the urfave default.
func resolveString(c *cli.Context, flag string, configVal string) string {
	if c.IsSet(flag) {
		return c.String(flag)
	}
	if configVal != "" {
		return configVal
	}
	return c.String(flag)
}

// resolveInt returns the CLI flag value if explicitly set, else the config
// value if non-zero, else the urfave default.
func resolveInt(c *cli.Context, flag string, configVal int) int {
	if c.IsSet(flag) {
		return c.Int(flag)
	}
	if configVal != 0 {
		return configVal
	}
	return c.Int(flag)
}

// resolveInt64 returns the CLI flag value if explicitly set, else the config
// value if non-zero, else the urfave default.
func resolveInt64(c *cli.Context, flag string, configVal int64) int64 {
	if c.IsSet(flag) {
		return c.Int64(flag)
	}
	if configVal != 0 {
		return configVal
	}
	return c.Int64(flag)
}

// resolveBool returns the CLI flag value if explicitly set, else the config
// value if true, else the urfave default.
func resolveBool(c *cli.Context, flag string, configVal bool) bool {
	if c.IsSet(flag) {
		return c.Bool(flag)
	}
	if configVal {
		return configVal
	}
	return c.Bool(flag)
}

// resolveDuration returns the CLI flag value if explicitly set, else the config
// value if non-zero, else the urfave default.
func resolveDuration(c *cli.Context, flag string, configVal time.Duration) time.Duration {
	if c.IsSet(flag) {
		return c.Duration(flag)
	}
	if configVal != 0 {
		return configVal
	}
	return c.Duration(flag)
}

// configVal safely extracts a string value from an optional config.
func configVal(cfg *quarryconfig.Config, fn func(*quarryconfig.Config) string) string {
	if cfg == nil {
		return ""
	}
	return fn(cfg)
}

// configIntVal safely extracts an int value from an optional config.
func configIntVal(cfg *quarryconfig.Config, fn func(*quarryconfig.Config) int) int {
	if cfg == nil {
		return 0
	}
	return fn(cfg)
}

// configInt64Val safely extracts an int64 value from an optional config.
func configInt64Val(cfg *quarryconfig.Config, fn func(*quarryconfig.Config) int64) int64 {
	if cfg == nil {
		return 0
	}
	return fn(cfg)
}

// configBoolVal safely extracts a bool value from an optional config.
func configBoolVal(cfg *quarryconfig.Config, fn func(*quarryconfig.Config) bool) bool {
	if cfg == nil {
		return false
	}
	return fn(cfg)
}

// configDurationVal extracts the adapter timeout duration from config.
func configDurationVal(cfg *quarryconfig.Config) time.Duration {
	if cfg == nil {
		return 0
	}
	return cfg.Adapter.Timeout.Duration
}

// configPolicyDurationVal extracts the policy flush interval from config.
func configPolicyDurationVal(cfg *quarryconfig.Config) time.Duration {
	if cfg == nil {
		return 0
	}
	return cfg.Policy.FlushInterval.Duration
}
