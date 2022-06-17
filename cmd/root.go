package cmd

import (
	"log"
	"os"
	"reflect"

	"github.com/spf13/cobra"

	"github.com/yoogottamk/sqlfs/pkg/fuse"
	"github.com/yoogottamk/sqlfs/pkg/sqlutils"
)

var sqlBackend string
var sqlDSN string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:              "sqlfs",
	Short:            "FUSE fs built on sql",
	PersistentPreRun: setFuseBackendFromFlag,
	Long: `A FUSE filesystem that stores information on SQL databases.

Currently only works with sqlite and mysql.`,
}

// set fuse backend from flag
func setFuseBackendFromFlag(cmd *cobra.Command, args []string) {
	backend, ok := sqlutils.AvaialableBackends[sqlBackend]
	if !ok {
		log.Fatalf("Unknown backend `%s`. Available backends: %s",
			sqlBackend,
			reflect.ValueOf(sqlutils.AvaialableBackends).MapKeys())
	}

	fuse.Backend = backend
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&sqlBackend, "backend", "b", "sqlite", "SQL backend to use [mysql|sqlite]")
	rootCmd.PersistentFlags().StringVarP(&sqlDSN, "dsn", "d", "fs.sql", "The DSN for connecting to the sql backend")
}
