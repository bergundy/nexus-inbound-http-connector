# Agent instructions

## Run the CI checks before committing

`.github/workflows/ci.yml` is the source of truth. Run every check it runs, in
this order, and fix what fails instead of committing around it:

```bash
golangci-lint run ./...                          # pinned to v2.13.2 in CI
test -z "$(gofmt -l .)"                          # formatting; `gofmt -w .` to fix
go vet ./...
go test -race ./...
go test -tags=integration -run TestIntegration ./...
```

The integration tests need no running `temporal` server: they start their own
development server through the Go SDK test suite, downloading the pinned
Temporal CLI into the user temp directory on first use.

When `ci.yml` changes, update this list to match.
