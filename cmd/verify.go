package cmd

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
	"github.com/yoogottamk/sqlfs/pkg/fuse"
)

// verifyCmd represents the verify command
var verifyCmd = &cobra.Command{
	Use:     "verify",
	Aliases: []string{"fsck"},
	Short:   "Verify the data stored in SQL database",
	Long:    "Verifies the data stored in SQL database",
	Run: func(cmd *cobra.Command, args []string) {
		if err := fuse.VerifyDB(sqlDSN); err != nil {
			log.Fatal(err)
		}

		fmt.Println("DB check finished successfully")
	},
}

func init() {
	rootCmd.AddCommand(verifyCmd)
}
