#!/usr/bin/env bash

set -eu

protoc -Isrc/server/grpc/proto/ --cpp_out=src/server/grpc/proto/ --grpc_out=src/server/grpc/proto/ --plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin index.proto
protoc -Isrc/server/grpc/proto/ --cpp_out=src/server/grpc/proto/ google/api/http.proto
protoc -Isrc/server/grpc/proto/ --cpp_out=src/server/grpc/proto/ google/api/annotations.proto
