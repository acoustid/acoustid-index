#!/bin/bash

# Setup NATS JetStream streams for development
# Run this after starting the docker-compose stack

set -e

echo "Setting up NATS JetStream streams for fpindex development..."

# Wait for NATS to be ready
echo "Waiting for NATS to be ready..."
until docker exec nats-server nats server ping > /dev/null 2>&1; do
    sleep 1
done
echo "NATS is ready!"

# Create the fpindex stream with compaction
echo "Creating fpindex stream..."
docker exec nats-server nats stream add fpindex \
  --subjects "fpindex.>" \
  --retention limits \
  --max-msgs-per-subject 1 \
  --max-msg-size 1MB \
  --max-age 7d \
  --storage file \
  --replicas 1 \
  --dupe-window 2m \
  --no-ack

echo "fpindex stream created successfully!"

# Show stream info
echo "Stream info:"
docker exec nats-server nats stream info fpindex

echo ""
echo "NATS JetStream setup complete!"
echo "You can now:"
echo "  - Test publishing: docker exec nats-server nats pub fpindex.test.123abc '{\"h\":[100,200,300]}'"
echo "  - View messages: docker exec nats-server nats stream view fpindex"
echo "  - Monitor: http://localhost:8222 (NATS monitoring)"
echo "  - fpindex API: http://localhost:8080/_health"