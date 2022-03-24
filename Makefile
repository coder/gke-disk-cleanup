all: clean fmt lint test build

clean:
	@echo "--- Cleaning up"
	rm -f ./pvc-cleanup

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
build: ./pvc-cleanup

./pvc-cleanup:
	@echo "--- Building"
	go build ./cmd/pvc-cleanup
