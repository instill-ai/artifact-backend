package logger

import (
	"context"
	"os"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/instill-ai/artifact-backend/config"
)

var once sync.Once
var core zapcore.Core

// GetZapLogger returns an instance of zap logger
// GetZapLogger returns an instance of zap logger
// It configures the logger based on the application's debug mode and sets up appropriate log levels and output destinations.
// The function also adds a hook to inject logs into OpenTelemetry traces.
func GetZapLogger(ctx context.Context) (*zap.Logger, error) {
	var err error
	once.Do(func() {
		// Enable debug and info level logs
		debugInfoLevel := zap.LevelEnablerFunc(func(level zapcore.Level) bool {
			return level == zapcore.DebugLevel || level == zapcore.InfoLevel
		})

		// Enable only info level logs
		infoLevel := zap.LevelEnablerFunc(func(level zapcore.Level) bool {
			return level == zapcore.InfoLevel
		})

		// Enable warn, error, and fatal level logs
		warnErrorFatalLevel := zap.LevelEnablerFunc(func(level zapcore.Level) bool {
			return level == zapcore.WarnLevel || level == zapcore.ErrorLevel || level == zapcore.FatalLevel
		})

		// Set up write syncers for stdout and stderr
		stdoutSyncer := zapcore.Lock(os.Stdout)
		stderrSyncer := zapcore.Lock(os.Stderr)

		// Configure the logger core based on debug mode
		if config.Config.Server.Debug {
			core = zapcore.NewTee(
				zapcore.NewCore(
					zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig()),
					stdoutSyncer,
					debugInfoLevel,
				),
				zapcore.NewCore(
					zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig()),
					stderrSyncer,
					warnErrorFatalLevel,
				),
			)
		} else {
			core = zapcore.NewTee(
				zapcore.NewCore(
					zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
					stdoutSyncer,
					infoLevel,
				),
				zapcore.NewCore(
					zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
					stderrSyncer,
					warnErrorFatalLevel,
				),
			)
		}
	})

	// Construct the logger with the configured core and add hooks
	logger := zap.New(core).WithOptions(
		zap.Hooks(func(entry zapcore.Entry) error {
			span := trace.SpanFromContext(ctx)
			if !span.IsRecording() {
				return nil
			}

			// Add log entry as an event to the current span
			span.AddEvent("log", trace.WithAttributes(
				attribute.KeyValue{
					Key:   "log.severity",
					Value: attribute.StringValue(entry.Level.String()),
				},
				attribute.KeyValue{
					Key:   "log.message",
					Value: attribute.StringValue(entry.Message),
				},
			))

			// Set span status based on log level
			if entry.Level >= zap.ErrorLevel {
				span.SetStatus(codes.Error, entry.Message)
			} else {
				span.SetStatus(codes.Ok, "")
			}

			return nil
		}),
		zap.AddCaller(),
		// Uncomment the following line to add stack traces for error logs
		// zap.AddStacktrace(zapcore.ErrorLevel),
	)

	return logger, err
}
