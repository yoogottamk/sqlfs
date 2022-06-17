package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/yoogottamk/sqlfs/cmd"
)

func main() {
	var progName = filepath.Base(os.Args[0] + ": ")

	log.SetFlags(0)
	log.SetPrefix(progName)

	cmd.Execute()
}
