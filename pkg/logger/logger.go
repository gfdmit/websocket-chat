package logger

import (
	"log"
	"os"
)

var (
	info *log.Logger
	warn *log.Logger
	err  *log.Logger
)

func Init() {
	info = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	warn = log.New(os.Stdout, "WARN: ", log.Ldate|log.Ltime|log.Lshortfile)
	err = log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
}

func Info(format string, v ...interface{}) {
	info.Printf(format, v...)
}

func Warn(format string, v ...interface{}) {
	warn.Printf(format, v...)
}

func Error(format string, v ...interface{}) {
	err.Printf(format, v...)
}
