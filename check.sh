#!/bin/bash
set -e

# Force rebuild and run unit tests by cleaning cache first
zig build unit-tests --summary all -freference-trace