// Command iceberg_exporter exposes Apache Iceberg table metadata as Prometheus
// metrics.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ragnard/iceberg_exporter/internal/config"
	"github.com/ragnard/iceberg_exporter/internal/metrics"
	"github.com/ragnard/iceberg_exporter/internal/registry"
	"github.com/ragnard/iceberg_exporter/internal/scrape"
)

var (
	version  = "dev"
	revision = "unknown"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "iceberg_exporter:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		configPath  string
		showVersion bool
	)
	flag.StringVar(&configPath, "c", "", "path to the configuration file")
	flag.StringVar(&configPath, "config", "", "path to the configuration file")
	flag.BoolVar(&showVersion, "version", false, "print version and compiled backends, then exit")
	flag.Parse()

	if showVersion {
		printVersion()
		return nil
	}
	if configPath == "" {
		flag.Usage()
		return errors.New("a configuration file is required (-c config.yaml)")
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	log := newLogger(cfg.Log)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Validates catalog selection and build tags without connecting; each
	// refresh opens (and closes) its own catalog clients.
	catalogs, err := scrape.NewCatalogs(cfg)
	if err != nil {
		return err
	}

	scraper, err := scrape.New(cfg, log, catalogs)
	if err != nil {
		return err
	}

	build := metrics.BuildInfo{
		Version:      version,
		Revision:     revision,
		Catalogs:     registry.CatalogTypes(),
		ObjectStores: registry.ObjectStoreSchemes(),
	}
	collector := metrics.NewCollector(scraper, build)
	scraper.SetTableErrorObserver(collector.ObserveTableError)

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collector,
		collector.TableErrors(),
	)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	// Probes reflect only the exporter process. Catalog and scrape failures
	// are reported through metrics, not by failing readiness.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Info("starting iceberg_exporter",
		"version", version,
		"revision", revision,
		"listen", cfg.Listen,
		"catalogs", strings.Join(catalogs.Names(), ","),
		"compiled_catalogs", strings.Join(build.Catalogs, ","),
		"object_stores", strings.Join(build.ObjectStores, ","),
		"interval", cfg.Scrape.Interval.Duration().String(),
	)

	scrapeCtx, cancelScrape := context.WithCancel(ctx)
	defer cancelScrape()
	scraperDone := make(chan struct{})
	go func() {
		defer close(scraperDone)
		scraper.Run(scrapeCtx)
	}()

	serverErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-serverErr:
		if err != nil {
			runErr = fmt.Errorf("http server: %w", err)
		}
	}

	log.Info("shutting down")
	cancelScrape()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && runErr == nil {
		runErr = fmt.Errorf("shutdown http server: %w", err)
	}
	select {
	case <-scraperDone:
	case <-shutdownCtx.Done():
		log.Warn("scraper did not stop before the shutdown deadline")
	}
	return runErr
}

func printVersion() {
	fmt.Printf("iceberg_exporter %s (%s)\n", version, revision)
	fmt.Printf("catalogs: %s\n", strings.Join(registry.CatalogTypes(), ", "))
	stores := registry.ObjectStoreSchemes()
	sort.Strings(stores)
	fmt.Printf("object stores: %s\n", strings.Join(stores, ", "))
}

func newLogger(cfg config.LogConfig) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(cfg.Level) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(cfg.Format, "json") {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}
