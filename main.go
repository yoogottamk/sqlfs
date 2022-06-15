package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/yoogottamk/sqlfs/fuse"
)

var invocatedAs = os.Args[0]
var progName = filepath.Base(invocatedAs)

func usage() {
	// TODO: implement for more backends (mysql, pg, etc) and add ability to choose
	fmt.Fprintf(os.Stderr, "Usage: %s SQLITE_FILE MOUNTPOINT\n", invocatedAs)
	flag.PrintDefaults()
}

func main() {
	log.SetFlags(0)
	log.SetPrefix(progName + ": ")

	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 2 {
		usage()
		os.Exit(2)
	}

	sqlDSN := flag.Arg(0)
	mountpoint := flag.Arg(1)

	if err := fuse.MountFS(sqlDSN, mountpoint); err != nil {
		log.Fatal(err)
	}
}
