// Package log provides structured logging with run context per CONTRACT_RUN.md.
package log

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/pithecene/quarry/types"
)

// Logger provides structured logging with run context.
// All log entries include run identity fields per CONTRACT_RUN.md.
type Logger struct {
	zap *zap.Logger
}

// NewLogger creates a new logger with run context.
// It writes to os.Stderr.
func NewLogger(runMeta *types.RunMeta) *Logger {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:     "timestamp",
		LevelKey:    "level",
		MessageKey:  "message",
		EncodeTime:  zapcore.RFC3339NanoTimeEncoder,
		EncodeLevel: zapcore.LowercaseLevelEncoder,
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(os.Stderr),
		zapcore.DebugLevel,
	)

	// Build run context fields per CONTRACT_RUN.md
	contextFields := []zap.Field{
		zap.String("run_id", runMeta.RunID),
		zap.Int("attempt", runMeta.Attempt),
	}
	if runMeta.JobID != nil {
		contextFields = append(contextFields, zap.String("job_id", *runMeta.JobID))
	}
	if runMeta.ParentRunID != nil {
		contextFields = append(contextFields, zap.String("parent_run_id", *runMeta.ParentRunID))
	}

	zapLogger := zap.New(core).With(contextFields...)
	return &Logger{zap: zapLogger}
}

// Debug logs a debug message.
func (l *Logger) Debug(message string, fields map[string]any) {
	l.zap.Debug(message, zap.Any("fields", fields))
}

// Info logs an info message.
func (l *Logger) Info(message string, fields map[string]any) {
	l.zap.Info(message, zap.Any("fields", fields))
}

// Warn logs a warning message.
func (l *Logger) Warn(message string, fields map[string]any) {
	l.zap.Warn(message, zap.Any("fields", fields))
}

// Error logs an error message.
func (l *Logger) Error(message string, fields map[string]any) {
	l.zap.Error(message, zap.Any("fields", fields))
}
