#!/usr/bin/env bash

set -o errexit
set -o pipefail

source ./env.sh

#gofumpt check
echo "Will use this command to check your local code: [find . -name "*.go" | grep -v "/vendor/" | xargs gofumpt -l -d]"
diff=`gofumpt -d -l .`
if [[ -n "${diff}" ]]; then
  echo "Gofumpt check failed since of:"
  echo "${diff}"
  echo "Please run this command to fix: [gofumpt -l -w]"
exit 1
fi

# go vet check
echo
echo "go vet check"
echo "Will use this command to check your local code: [go vet -tags="dev embed" ./...]"
source ./env-server.sh
go vet -tags="dev embed" -unsafeptr=false `go list ./...| grep -v "/vendor/"`
if [[ $? -ne 0 ]];then
  echo "go vet failed. exit"
  exit 1
fi

# golangci-lint check
echo
echo "golangci-lint check"
golangci-lint run --timeout 10m --issues-exit-code=1 -D errcheck -E bodyclose ./...
if [[ $? -ne 0 ]];then
  echo "golangci-lint failed. exit"
  exit 1
fi

# go test run
echo
echo "go test"
#go test -gcflags "all=-N -l" `go list ./...|grep -v "/vendor/"`
source ./env.sh
go test -gcflags "all=-N -l" --count=1 -cover `go list ./...|grep -v "/vendor/"`

echo
echo "success"
