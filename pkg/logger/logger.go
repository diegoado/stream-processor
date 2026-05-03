package logger

import (
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/contrib/bridges/otelslog"
)

func init() {
	var handler slog.Handler

	opts := &slog.HandlerOptions{Level: slog.LevelInfo}

	if isRemoveEnvironment() {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	slog.SetDefault(slog.New(handler))
}

// Get returns a named logger derived from the default slog instance.
func Get(name string) *slog.Logger {
	return slog.Default().With("name", name)
}

// SetOTelHandler adds the OpenTelemetry bridge alongside the existing handler.
func SetOTelHandler(serviceName string) {
	slog.SetDefault(slog.New(newCompositeHandler(slog.Default().Handler(), otelslog.NewHandler(serviceName))))
}

func isRemoveEnvironment() bool {
	env := strings.ToLower(os.Getenv("ENVIRONMENT"))
	return env == "prod" || env == "sandbox"
}
