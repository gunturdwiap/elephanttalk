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
	"syscall"
	"time"

	"github.com/gunturdwiap/elephanttalk/ws/v3"
)

type application struct {
	hub    *ws.Hub
	logger *slog.Logger
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	if err := run(logger); err != nil {
		logger.Error("application stopped with error", slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	var port int
	flag.IntVar(&port, "port", 8080, "server port")
	flag.Parse()

	app := &application{
		hub:    ws.NewHub(),
		logger: logger,
	}

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: app.routes(),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serverError := make(chan error, 1)
	go func() {
		logger.Info("starting server", slog.Int("port", port))
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			serverError <- err
		}
	}()

	select {
	case err := <-serverError:
		return fmt.Errorf("shutting down server error: %w", err)
	case <-ctx.Done():
		logger.Info("shutting down server")
	}

	app.hub.Shutdown()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http server forced shutdown: %w", err)
	}

	logger.Info("server stopped", slog.Int("port", port))
	return nil
}
