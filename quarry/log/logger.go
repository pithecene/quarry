// Package log provides structured logging with run context per CONTRACT_RUN.md.
//
// Two logger variants are available:
//   - Logger: Non-sugared zap.Logger for core runtime (high performance, structured fields)
//   - SugaredLogger: Printf-style logging for CLI/debug surfaces (convenience over performance)
//
// Use Logger.Sugar() to obtain a SugaredLogger when needed.
package log

import (
	"io"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/pithecene/quarry/types"
)

// Logger provides structured logging with run context.
// All log entries include run identity fields per CONTRACT_RUN.md.
//
// Use this for core runtime paths where performance matters.
// For CLI/debug surfaces, use Sugar() to get a SugaredLogger.
type Logger struct {
	zap *zap.Logger
}

// SugaredLogger provides printf-style logging for CLI and debug surfaces.
// Wraps zap.SugaredLogger with run context.
//
// Use this for CLI output, debug logging, and surfaces where convenience
// matters more than performance.
type SugaredLogger struct {
	sugar *zap.SugaredLogger
}

// NewLogger creates a new logger with run context.
// Output defaults to os.Stderr.
func NewLogger(runMeta *types.RunMeta) *Logger {
	return newLoggerWithWriter(runMeta, os.Stderr)
}

// WithOutput returns a new logger with a different output writer.
func (l *Logger) WithOutput(w io.Writer) *Logger {
	// Clone with new core pointing to new writer
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:     "timestamp",
		LevelKey:    "level",
		MessageKey:  "message",
		EncodeTime:  zapcore.RFC3339NanoTimeEncoder,
		EncodeLevel: zapcore.LowercaseLevelEncoder,
	}
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(w),
		zapcore.DebugLevel,
	)
	return &Logger{zap: l.zap.WithOptions(zap.WrapCore(func(zapcore.Core) zapcore.Core { return core }))}
}

// newLoggerWithWriter creates a logger writing to the specified writer.
func newLoggerWithWriter(runMeta *types.RunMeta, w io.Writer) *Logger {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:     "timestamp",
		LevelKey:    "level",
		MessageKey:  "message",
		EncodeTime:  zapcore.RFC3339NanoTimeEncoder,
		EncodeLevel: zapcore.LowercaseLevelEncoder,
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(w),
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

// Sugar returns a SugaredLogger for printf-style logging.
// Use for CLI/debug surfaces where convenience matters more than performance.
func (l *Logger) Sugar() *SugaredLogger {
	return &SugaredLogger{sugar: l.zap.Sugar()}
}

// Debugf logs a debug message with printf-style formatting.
func (s *SugaredLogger) Debugf(template string, args ...any) {
	s.sugar.Debugf(template, args...)
}

// Infof logs an info message with printf-style formatting.
func (s *SugaredLogger) Infof(template string, args ...any) {
	s.sugar.Infof(template, args...)
}

// Warnf logs a warning message with printf-style formatting.
func (s *SugaredLogger) Warnf(template string, args ...any) {
	s.sugar.Warnf(template, args...)
}

// Errorf logs an error message with printf-style formatting.
func (s *SugaredLogger) Errorf(template string, args ...any) {
	s.sugar.Errorf(template, args...)
}

// With returns a SugaredLogger with additional context fields.
func (s *SugaredLogger) With(args ...any) *SugaredLogger {
	return &SugaredLogger{sugar: s.sugar.With(args...)}
}
