package cmd

import (
	"log"

	"github.com/spf13/cobra"

	"github.com/yoogottamk/sqlfs/pkg/fuse"
)

// mountCmd represents the mount command
//
// Mounts the fuse fs after verification
var mountCmd = &cobra.Command{
	Use:   "mount [flags] MOUNTPOINT",
	Short: "Mount the FUSE fs",
	Long: `Mounts the FUSE fs.

Verifies the DB tables/rows and mounts it.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if err := fuse.MountFS(sqlDSN, args[0]); err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(mountCmd)
}
