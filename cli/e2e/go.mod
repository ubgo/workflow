module github.com/ubgo/workflow/cli/e2e

go 1.24.0

require (
	github.com/ubgo/workflow v0.0.0
	github.com/ubgo/workflow/admin v0.0.0
	github.com/ubgo/workflow/cli v0.0.0
)

replace (
	github.com/ubgo/workflow => ../../
	github.com/ubgo/workflow/admin => ../../admin
	github.com/ubgo/workflow/cli => ../
)
