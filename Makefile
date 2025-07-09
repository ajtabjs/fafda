BINARY_NAME=fafda

export CGO_ENABLED=0

tidy:
	go fmt ./...
	go mod tidy -v

audit:
	go mod verify
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@latest -checks=all,-ST1000,-U1000 ./...
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...
	go test -race -buildvcs -vet=off ./...

test:
	go test -v -race -buildvcs ./...

build:
	go build -ldflags="-s -w" -o ./bin/$(BINARY_NAME) ./cmd/fafda

run: build
	./bin/$(BINARY_NAME) --debug

clean:
	rm -rf ./bin/*

build-linux:
	GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o bin/release/$(BINARY_NAME) ./cmd/fafda

build-windows:
	set GOOS=windows&& set GOARCH=amd64&& go build -ldflags="-s -w" -o bin/release/$(BINARY_NAME).exe ./cmd/fafda

build-linux-arm64:
	GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o bin/release/$(BINARY_NAME)-linux-arm64 ./cmd/fafda

