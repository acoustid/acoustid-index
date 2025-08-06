#!/bin/bash

# Test script for the docker-compose setup
# Run this with: ./test-setup.sh

set -e

echo "🚀 Testing Docker Compose Setup for fpindex + NATS"
echo "=================================================="

# Clean up any existing containers
echo "Cleaning up existing containers..."
docker compose down -v || true

echo ""
echo "Starting services..."
docker compose up -d

echo ""
echo "Waiting for services to be ready..."
sleep 10

# Check if containers are running
echo ""
echo "Checking container status..."
docker compose ps

# Wait for NATS to be ready
echo ""
echo "Waiting for NATS to be ready..."
max_attempts=30
attempt=0
while ! docker exec nats-server nats server ping >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ $attempt -ge $max_attempts ]; then
        echo "❌ NATS failed to start after $max_attempts attempts"
        docker compose logs nats
        exit 1
    fi
    echo "  Attempt $attempt/$max_attempts - waiting for NATS..."
    sleep 2
done
echo "✅ NATS is ready!"

# Setup NATS streams
echo ""
echo "Setting up NATS JetStream streams..."
./dev/setup-nats-streams.sh

# Wait for fpindex to be ready
echo ""
echo "Waiting for fpindex to be ready..."
max_attempts=30
attempt=0
while ! curl -s http://localhost:6081/_health >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ $attempt -ge $max_attempts ]; then
        echo "❌ fpindex failed to start after $max_attempts attempts"
        docker compose logs fpindex
        exit 1
    fi
    echo "  Attempt $attempt/$max_attempts - waiting for fpindex..."
    sleep 2
done
echo "✅ fpindex is ready!"

# Test fpindex API
echo ""
echo "Testing fpindex API..."
if python3 dev/test-fpindex-api.py; then
    echo "✅ fpindex API tests passed!"
else
    echo "❌ fpindex API tests failed!"
    exit 1
fi

# Test NATS functionality
echo ""
echo "Testing NATS functionality..."

# Publish a test message
echo "Publishing test message to NATS..."
docker exec nats-server nats pub fpindex.test.deadbeef '{"h":[100,200,300,400,500]}' || {
    echo "❌ Failed to publish to NATS"
    exit 1
}

# Check stream info
echo "Checking NATS stream info..."
docker exec nats-server nats stream info fpindex || {
    echo "❌ Failed to get stream info"
    exit 1
}

# View messages in stream
echo "Messages in fpindex stream:"
docker exec nats-server nats stream view fpindex --raw || {
    echo "❌ Failed to view stream messages"
    exit 1
}

echo ""
echo "🎉 All tests passed!"
echo ""
echo "🔗 Useful URLs:"
echo "  - fpindex API: http://localhost:6081/_health"
echo "  - NATS monitoring: http://localhost:8222"
echo ""
echo "🛠  Management commands:"
echo "  - NATS CLI: docker exec -it nats-box sh"
echo "  - View logs: docker compose logs <service>"
echo "  - Stop services: docker compose down"
echo ""
echo "✅ Development environment is ready!"