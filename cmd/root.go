package cmd

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"

	"github.com/spf13/cobra"

	"github.com/yoogottamk/sqlfs/pkg/fuse"
	"github.com/yoogottamk/sqlfs/pkg/sqlutils"
)

var sqlURI string
var sqlDSN string

var availableBackends = reflect.ValueOf(sqlutils.AvaialableBackends).MapKeys()

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:              "sqlfs",
	Short:            "FUSE fs built on sql",
	PersistentPreRun: setFuseBackendFromFlag,
	Long: fmt.Sprintf(`A FUSE filesystem that stores information on SQL databases.

Available backends: %s

* DSN for sqlite: filepath

* DSN for mysql: [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]

    For example, if mysql is running on localhost and
    username=user, password=password and dbname is sqlfs, DSN would be:

        user:password@/sqlfs

    More information can be found here: https://github.com/go-sql-driver/mysql

* DSN for postgres: username:password@address/dbname
`,
		availableBackends),
}

// set fuse backend from flag
func setFuseBackendFromFlag(cmd *cobra.Command, args []string) {
	if !strings.Contains(sqlURI, "://") {
		log.Fatalf(`uri must be of the form backend://dsn`)
	}

	uriParts := strings.Split(sqlURI, "://")
	sqlBackend := uriParts[0]
	sqlDSN = uriParts[1]

	backend, ok := sqlutils.AvaialableBackends[sqlBackend]
	if !ok {
		log.Fatalf("Unknown backend `%s`. Available backends: %s", sqlURI, availableBackends)
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
	rootCmd.PersistentFlags().StringVarP(&sqlURI, "uri", "u", "sqlite://fs.sql", "SQL URI to connect to [backend://dsn]")
}
