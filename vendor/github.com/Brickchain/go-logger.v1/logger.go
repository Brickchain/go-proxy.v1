/*
The package Logger is a wrapper for the Logrus logger package.

Logger is used by most Brickchain software to enable context based logging with details needed by each component.


Installation

	# How to install logger
	$ go get github.com/Brickchain/go-logger.v1


Example

	logger.SetOutput(os.Stdout)
	logger.SetFormatter("text")
	logger.SetLevel("debug")
	logger.AddContext("service", path.Base(os.Args[0]))
	logger.AddContext("version", constant.Version)

	logger.Infof("Running version %s", version)
	logger.Error(err)
*/
package logger

import (
	"context"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/sirupsen/logrus"
)

var (
	ctxlogger *logrus.Entry
	mu        *sync.Mutex
)

// Entry contains a Logrus entry
type Entry struct {
	entry *logrus.Entry
}

// Fields is a map of fields used by the logger
type Fields map[string]interface{}

func init() {
	mu = &sync.Mutex{}
	mu.Lock()
	defer mu.Unlock()
	logrus.SetOutput(os.Stdout)
	hostname, _ := os.Hostname()
	ctxlogger = logrus.WithField("pid", os.Getpid()).WithField("hostname", hostname)

}

// GetLogger returns the context logger
func GetLogger() *logrus.Entry {
	return ctxlogger
}

// AddContext adds another context to the logger
func AddContext(key string, value interface{}) {
	mu.Lock()
	defer mu.Unlock()
	ctxlogger = ctxlogger.WithField(key, value)
}

// SetFormatter sets the formatter to be used, either "json" or "text"
func SetFormatter(formatter string) {
	var _formatter logrus.Formatter
	switch formatter {
	case "json":
		_formatter = &logrus.JSONFormatter{}
	default:
		_formatter = &logrus.TextFormatter{}
	}
	mu.Lock()
	defer mu.Unlock()
	data := ctxlogger.Data
	logrus.SetFormatter(_formatter)
	ctxlogger = logrus.WithFields(data)
}

// SetOutput sets the io.Writer to use
func SetOutput(out io.Writer) {
	mu.Lock()
	defer mu.Unlock()
	data := ctxlogger.Data
	logrus.SetOutput(out)
	ctxlogger = logrus.WithFields(data)
}

// SetLevel sets the lowest level to log
func SetLevel(level string) {
	_level, err := logrus.ParseLevel(level)
	if err != nil {
		_level = logrus.InfoLevel
	}
	mu.Lock()
	defer mu.Unlock()
	data := ctxlogger.Data
	logrus.SetLevel(_level)
	ctxlogger = logrus.WithFields(data)
}

// GetLoglevel returns the current log level
func GetLoglevel() string {
	return logrus.GetLevel().String()
}

// AddField adds a field to the current context
func (e *Entry) AddField(key string, value interface{}) {
	mu.Lock()
	defer mu.Unlock()
	e.entry = e.entry.WithField(key, value)
}

// WithField adds another field to the logger entry context
func WithField(key string, value interface{}) *Entry {
	return &Entry{
		entry: ctxlogger.WithField(key, value),
	}
}

// WithFields adds more Fields to the logger entry context
func WithFields(fields Fields) *Entry {
	_fields := logrus.Fields{}
	for k, v := range fields {
		_fields[k] = v
	}
	return &Entry{
		entry: ctxlogger.WithFields(_fields),
	}
}

// Debug is the wrapper for Logrus Debug()
func Debug(args ...interface{}) {
	loggerWithCaller().Debug(args...)
}

// Info is the wrapper for Logrus Info()
func Info(args ...interface{}) {
	loggerWithCaller().Info(args...)
}

// Warn is the wrapper for Logrus Warn()
func Warn(args ...interface{}) {
	loggerWithCaller().Warn(args...)
}

// Error is the wrapper for Logrus Error()
func Error(args ...interface{}) {
	loggerWithCaller().Error(args...)
}

// Fatal is the wrapper for Logrus Fatal()
func Fatal(args ...interface{}) {
	loggerWithCaller().Fatal(args...)
}

// Fatalf is the wrapper for Logris Fatalf()
func Fatalf(format string, args ...interface{}) {
	loggerWithCaller().Fatalf(format, args...)
}

// Errorf is the wrapper for Logrus Errorf()
func Errorf(format string, args ...interface{}) {
	loggerWithCaller().Errorf(format, args...)
}

// Infof is the wrapper for Logrus Infof()
func Infof(format string, args ...interface{}) {
	loggerWithCaller().Infof(format, args...)
}

// Warningf is the wrapper for Logrus Warningf()
func Warningf(format string, args ...interface{}) {
	loggerWithCaller().Warningf(format, args...)
}

// Debugf is the wrapper for Logrus Debugf()
func Debugf(format string, args ...interface{}) {
	loggerWithCaller().Debugf(format, args...)
}

func loggerWithCaller() *logrus.Entry {
	_, file, line, _ := runtime.Caller(2)
	fields := logrus.Fields{
		"file": file,
		"line": line,
	}
	return ctxlogger.WithFields(fields)
}

// ForContext TODO docs
func ForContext(ctx context.Context) *Entry {
	reqID, _ := ctx.Value(0).(string)
	return &Entry{
		entry: ctxlogger.WithField("request-id", reqID),
	}
}

// WithField adds another field to the Logrus entry context
func (e *Entry) WithField(key string, value interface{}) *Entry {
	e.entry = e.entry.WithField(key, value)
	return e
}

// WithFields adds more Fields to the Logrus entry context
func (e *Entry) WithFields(fields Fields) *Entry {
	_fields := logrus.Fields{}
	for k, v := range fields {
		_fields[k] = v
	}
	e.entry = e.entry.WithFields(_fields)
	return e
}

// Debug is the wrapper for Logrus Debug()
func (e *Entry) Debug(args ...interface{}) {
	e.loggerWithCaller().entry.Debug(args...)
}

// Info is the wrapper for Logrus Info()
func (e *Entry) Info(args ...interface{}) {
	e.loggerWithCaller().entry.Info(args...)
}

// Warn is the wrapper for Logrus Warn()
func (e *Entry) Warn(args ...interface{}) {
	e.loggerWithCaller().entry.Warn(args...)
}

// Error is the wrapper for Logrus Error()
func (e *Entry) Error(args ...interface{}) {
	e.loggerWithCaller().entry.Error(args...)
}

// Fatal is the wrapper for Logrus Fatal()
func (e *Entry) Fatal(args ...interface{}) {
	e.loggerWithCaller().entry.Fatal(args...)
}

// Errorf is the wrapper for Logrus Errorf()
func (e *Entry) Errorf(format string, args ...interface{}) {
	e.loggerWithCaller().entry.Errorf(format, args...)
}

// Infof is the wrapper for Logrus Infof()
func (e *Entry) Infof(format string, args ...interface{}) {
	e.loggerWithCaller().entry.Infof(format, args...)
}

// Warningf is the wrapper for Logrus Warninf()
func (e *Entry) Warningf(format string, args ...interface{}) {
	e.loggerWithCaller().entry.Warningf(format, args...)
}

// Debugf is the wrapper for Logrus Debugf()
func (e *Entry) Debugf(format string, args ...interface{}) {
	e.loggerWithCaller().entry.Debugf(format, args...)
}

func (e *Entry) loggerWithCaller() *Entry {
	_, file, line, _ := runtime.Caller(2)
	fields := logrus.Fields{
		"file": file,
		"line": line,
	}
	n := &*e
	n.entry = n.entry.WithFields(fields)
	return n
}
