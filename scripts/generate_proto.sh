#!/usr/bin/env bash

# Assuming script is placed inside a directory whose parent is the project root
PROJECT_ROOT=$( dirname $(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd) )

PROTO_DIR=grpc

protoc -I ${PROJECT_ROOT} \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    ${PROTO_DIR}/replication.proto

