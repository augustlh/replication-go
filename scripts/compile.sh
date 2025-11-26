#/usr/bin/env bash

PROJECT_ROOT=$(dirname $( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd ))

set -xeu pipefail

go build -o $PROJECT_ROOT/ChitChatClient $PROJECT_ROOT/client/*.go
go build -o $PROJECT_ROOT/ChitChatServer $PROJECT_ROOT/server/*.go

