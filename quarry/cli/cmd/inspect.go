package cmd

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/cli/reader"
	"github.com/pithecene/quarry/cli/render"
)

// InspectCommand returns the inspect command with subcommands.
// Inspect returns a deep view of a single entity per CONTRACT_CLI.md.
func InspectCommand() *cli.Command {
	return &cli.Command{
		Name:  "inspect",
		Usage: "Inspect a single entity (run, job, task, proxy, executor)",
		Subcommands: []*cli.Command{
			inspectRunCommand(),
			inspectJobCommand(),
			inspectTaskCommand(),
			inspectProxyCommand(),
			inspectExecutorCommand(),
		},
	}
}

func inspectRunCommand() *cli.Command {
	return &cli.Command{
		Name:      "run",
		Usage:     "Inspect a run by ID",
		ArgsUsage: "<run-id>",
		Flags:     TUIReadOnlyFlags(),
		Action:    inspectRunAction,
	}
}

func inspectRunAction(c *cli.Context) error {
	if c.NArg() < 1 {
		return cli.Exit("run-id required", 1)
	}
	runID := c.Args().First()

	// Get renderer
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	// Handle TUI mode
	if c.Bool("tui") {
		return r.RenderTUI("inspect_run", reader.InspectRun(runID))
	}

	// Standard render
	resp := reader.InspectRun(runID)
	return r.Render(resp)
}

func inspectJobCommand() *cli.Command {
	return &cli.Command{
		Name:      "job",
		Usage:     "Inspect a job by ID",
		ArgsUsage: "<job-id>",
		Flags:     TUIReadOnlyFlags(),
		Action:    inspectJobAction,
	}
}

func inspectJobAction(c *cli.Context) error {
	if c.NArg() < 1 {
		return cli.Exit("job-id required", 1)
	}
	jobID := c.Args().First()

	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	if c.Bool("tui") {
		return r.RenderTUI("inspect_job", reader.InspectJob(jobID))
	}

	return r.Render(reader.InspectJob(jobID))
}

func inspectTaskCommand() *cli.Command {
	return &cli.Command{
		Name:      "task",
		Usage:     "Inspect a task by ID",
		ArgsUsage: "<task-id>",
		Flags:     TUIReadOnlyFlags(),
		Action:    inspectTaskAction,
	}
}

func inspectTaskAction(c *cli.Context) error {
	if c.NArg() < 1 {
		return cli.Exit("task-id required", 1)
	}
	taskID := c.Args().First()

	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	if c.Bool("tui") {
		return r.RenderTUI("inspect_task", reader.InspectTask(taskID))
	}

	return r.Render(reader.InspectTask(taskID))
}

func inspectProxyCommand() *cli.Command {
	return &cli.Command{
		Name:      "proxy",
		Usage:     "Inspect a proxy pool by name",
		ArgsUsage: "<pool-name>",
		Flags:     TUIReadOnlyFlags(),
		Action:    inspectProxyAction,
	}
}

func inspectProxyAction(c *cli.Context) error {
	if c.NArg() < 1 {
		return cli.Exit("pool-name required", 1)
	}
	poolName := c.Args().First()

	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	if c.Bool("tui") {
		return r.RenderTUI("inspect_proxy", reader.InspectProxy(poolName))
	}

	return r.Render(reader.InspectProxy(poolName))
}

func inspectExecutorCommand() *cli.Command {
	return &cli.Command{
		Name:      "executor",
		Usage:     "Inspect an executor by ID",
		ArgsUsage: "<executor-id>",
		Flags:     TUIReadOnlyFlags(),
		Action:    inspectExecutorAction,
	}
}

func inspectExecutorAction(c *cli.Context) error {
	if c.NArg() < 1 {
		return cli.Exit("executor-id required", 1)
	}
	executorID := c.Args().First()

	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	if c.Bool("tui") {
		return r.RenderTUI("inspect_executor", reader.InspectExecutor(executorID))
	}

	resp := reader.InspectExecutor(executorID)
	if resp == nil {
		return fmt.Errorf("executor not found: %s", executorID)
	}

	return r.Render(resp)
}
