package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	orderfulfillment "github.com/spapa/orchid/examples/order_fulfillment"
	"github.com/spapa/orchid/pkg/client"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		return
	}
	baseURL := os.Getenv("ORCHID_ADDR")
	if baseURL == "" {
		baseURL = "http://127.0.0.1:8080"
	}
	cli := client.New(baseURL)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	switch os.Args[1] {
	case "workflows":
		workflows, err := cli.Workflows(ctx)
		exitOnErr(err)
		for _, workflowName := range workflows {
			fmt.Println(workflowName)
		}
	case "start":
		cmd := flag.NewFlagSet("start", flag.ExitOnError)
		workflowName := cmd.String("workflow", "order_fulfillment", "workflow name")
		inputPath := cmd.String("input", "", "path to JSON input file")
		useExample := cmd.Bool("example", true, "use built-in order fulfillment payload when no file is provided")
		_ = cmd.Parse(os.Args[2:])

		var payload any
		switch {
		case *inputPath != "":
			body, err := os.ReadFile(*inputPath)
			exitOnErr(err)
			if err := json.Unmarshal(body, &payload); err != nil {
				exitOnErr(err)
			}
		case *useExample:
			payload = orderfulfillment.ExampleInput()
		default:
			payload = map[string]any{}
		}
		run, err := cli.StartRun(ctx, *workflowName, payload, map[string]string{"source": "orchctl"})
		exitOnErr(err)
		printJSON(run)
	case "list":
		runs, err := cli.ListRuns(ctx, 25)
		exitOnErr(err)
		printJSON(runs)
	case "get":
		requireArg(3)
		view, err := cli.GetRun(ctx, os.Args[2])
		exitOnErr(err)
		printJSON(view)
	case "history":
		requireArg(3)
		events, err := cli.History(ctx, os.Args[2])
		exitOnErr(err)
		printJSON(events)
	case "replay":
		requireArg(3)
		report, err := cli.Replay(ctx, os.Args[2])
		exitOnErr(err)
		printJSON(report)
	case "cancel":
		requireArg(3)
		reason := "cancelled by orchctl"
		if len(os.Args) > 3 {
			reason = os.Args[3]
		}
		exitOnErr(cli.Cancel(ctx, os.Args[2], reason))
		fmt.Println("cancellation requested")
	default:
		usage()
	}
}

func usage() {
	fmt.Println("orchctl workflows")
	fmt.Println("orchctl start [-workflow name] [-input file]")
	fmt.Println("orchctl list")
	fmt.Println("orchctl get <run-id>")
	fmt.Println("orchctl history <run-id>")
	fmt.Println("orchctl replay <run-id>")
	fmt.Println("orchctl cancel <run-id> [reason]")
}

func requireArg(min int) {
	if len(os.Args) < min {
		usage()
		os.Exit(1)
	}
}

func printJSON(value any) {
	body, err := json.MarshalIndent(value, "", "  ")
	exitOnErr(err)
	fmt.Println(string(body))
}

func exitOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
