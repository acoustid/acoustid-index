#!/usr/bin/env bash

set -eu

protoc --cpp_out=src/server/grpc/proto/ --grpc_out=src/server/grpc/proto/ --plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin -Isrc/server/grpc/proto/ index.proto
