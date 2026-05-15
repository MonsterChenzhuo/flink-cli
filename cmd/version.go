package cmd

import "github.com/spf13/cobra"

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print flink-cli version",
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.Printf("flink-cli %s\n", version)
			return nil
		},
	}
}
