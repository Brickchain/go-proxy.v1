# Logging framework for Integrity components

This logger components is used by Integrity components that logs output.

Based on [Logrus](https://github.com/Sirupsen/logrus).

It manages logger context and settings on a global level by using the package level functions.

## Usage
```go
package main

import (
  "github.com/brickchain/go-logger.v1"
  "os"
)

func main() {
  logger.SetOutput(os.Stdout)
  logger.SetFormatter("json")
  logger.SetLevel("debug")
  
  logger.AddContext("some_key", "some_value")
   
  localLogger := logger.WithField("my_local_key", "my_local_value")
  localLogger.Info("Test")
}
```
