package main

import (
	"os"

	"github.com/ayeshLK/websubhub/internal/command"
)

func main() {
	os.Exit(command.Run("websubhub", os.Args[1:], os.Stdout, os.Stderr))
}
