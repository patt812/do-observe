package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "report" {
		runReport(os.Args[2:])
		return
	}
	runProber(os.Args[1:])
}

func runProber(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	configPath := fs.String("config", "config.json", "path to config file")
	fs.Parse(args)

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}

	events, err := NewEventWriter(cfg.DataDir, cfg.Site)
	if err != nil {
		fmt.Fprintf(os.Stderr, "events: %v\n", err)
		os.Exit(1)
	}
	defer events.Close()

	metrics := NewMetrics(cfg.Site, cfg)

	totalConns := 0
	for _, co := range cfg.Cohorts {
		totalConns += co.Dos * co.ConnsPerDo
	}
	events.Emit("prober_start", map[string]any{
		"workerUrl": cfg.WorkerURL, "totalConns": totalConns, "cohorts": cfg.Cohorts,
	})
	fmt.Printf("do-bench prober: site=%s conns=%d metrics=%s data=%s\n",
		cfg.Site, totalConns, cfg.MetricsAddr, cfg.DataDir)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// /metrics (Prometheus text format) — Grafana Alloy 等でscrapeする
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", metrics.Handler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	srv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux}
	go srv.ListenAndServe()
	defer srv.Shutdown(context.Background())

	var wg sync.WaitGroup
	for _, d := range cfg.doNames() {
		nConns := 0
		for _, co := range cfg.Cohorts {
			if co.Prefix == d.Cohort {
				nConns = co.ConnsPerDo
			}
		}
		m := NewMonitor(cfg, d.Name, d.Cohort, events, metrics.Get(d.Name), nConns)
		m.Run(ctx, &wg)
	}

	// 定期処理: statsスナップショット(5分) + healthchecksピング(1分)
	go func() {
		statsTick := time.NewTicker(5 * time.Minute)
		healthTick := time.NewTicker(1 * time.Minute)
		defer statsTick.Stop()
		defer healthTick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-statsTick.C:
				events.Emit("stats", map[string]any{"connected": metrics.snapshotConnected()})
			case <-healthTick.C:
				pingHealthcheck(cfg.HealthcheckURL)
			}
		}
	}()

	<-ctx.Done()
	fmt.Println("shutting down...")
	shutdownDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownDone)
	}()
	select {
	case <-shutdownDone:
	case <-time.After(10 * time.Second):
	}
	events.Emit("prober_stop", map[string]any{})
}

func pingHealthcheck(url string) {
	if url == "" {
		return
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return
	}
	resp.Body.Close()
}
