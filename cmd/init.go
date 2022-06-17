package cmd

import (
	"log"

	"github.com/spf13/cobra"

	"github.com/yoogottamk/sqlfs/pkg/fuse"
)

// initCmd represents the init command
var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize the SQL db",
	Long: `Initializes the SQL db

Creates necessary tables and adds the rootdir inode entry`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := fuse.InitializeDB(sqlDSN); err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
}
