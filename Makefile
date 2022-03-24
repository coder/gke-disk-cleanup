all: clean fmt lint test build

clean:
	@echo "--- Cleaning up"
	rm -f ./gce-disk-cleanup

.PHONY: fmt
fmt:
	@echo "--- Formatting"
	git ls-files '*.go' | xargs goimports -w

.PHONY: lint
lint:
	@echo "--- Linting"
	golangci-lint run

.PHONY: test
test:
	@echo "--- Testing"
	go test ./...

.PHONY: build
build: ./gce-disk-cleanup

./gce-disk-cleanup:
	@echo "--- Building"
	go build ./cmd/gce-disk-cleanup
