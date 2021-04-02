package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/cmdutils"
	"github.com/treeverse/lakefs/pkg/uri"
)

const (
	mergeCmdMinArgs = 2
	mergeCmdMaxArgs = 2
)

var mergeCreateTemplate = `Merged "{{.Merge.FromRef|yellow}}" into "{{.Merge.ToRef|yellow}}" to get "{{.Result.Reference|green}}".

Added: {{.Result.Summary.Added}}
Changed: {{.Result.Summary.Changed}}
Removed: {{.Result.Summary.Removed}}

`

type FromTo struct {
	FromRef, ToRef string
}

// mergeCmd represents the merge command
var mergeCmd = &cobra.Command{
	Use:   "merge <source ref> <destination ref>",
	Short: "merge",
	Long:  "merge & commit changes from source branch into destination branch",
	Args: cmdutils.ValidationChain(
		cobra.RangeArgs(mergeCmdMinArgs, mergeCmdMaxArgs),
		cmdutils.FuncValidator(0, uri.ValidateRefURI),
		cmdutils.FuncValidator(1, uri.ValidateRefURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		sourceRef := uri.Must(uri.Parse(args[0]))
		destinationRef := uri.Must(uri.Parse(args[1]))

		if destinationRef.Repository != sourceRef.Repository {
			Die("both references must belong to the same repository", 1)
		}

		resp, err := client.MergeIntoBranchWithResponse(cmd.Context(), destinationRef.Repository, sourceRef.Ref, destinationRef.Ref, api.MergeIntoBranchJSONRequestBody{})
		if resp != nil && resp.JSON409 != nil {
			_, _ = fmt.Printf("Conflicts: %d\n", resp.JSON409.Summary.Conflict)
			return
		}
		DieOnResponseError(resp, err)

		Write(mergeCreateTemplate, struct {
			Merge  FromTo
			Result *api.MergeResult
		}{
			Merge:  FromTo{FromRef: sourceRef.Ref, ToRef: destinationRef.Ref},
			Result: resp.JSON200,
		})
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(mergeCmd)
}
