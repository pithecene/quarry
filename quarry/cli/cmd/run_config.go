package cmd

import "github.com/urfave/cli/v2"

// resolve returns the flag's value if it is explicitly set, else the configured
// value if it is not zero, else the flag's default.
func resolve[T comparable](c *cli.Context, flag string, configured T, get func(*cli.Context, string) T) T {
	var zero T
	if !c.IsSet(flag) && configured != zero {
		return configured
	}
	return get(c, flag)
}
