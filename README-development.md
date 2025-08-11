# Development Setup

This document describes how to set up a local development environment for testing the NATS-based fpindex synchronization system.

## Quick Start

1. **Build fpindex binary** (if not already built):
   ```bash
   zig build
   ```

2. **Start the development environment**:
   ```bash
   docker-compose up -d
   ```

3. **Setup NATS JetStream**:
   ```bash
   ./dev/setup-nats-streams.sh
   ```

4. **Test fpindex API**:
   ```bash
   pip install -r dev/requirements.txt
   python dev/test-fpindex-api.py
   ```

## Services

### fpindex (Port 6081)
- **Health**: http://localhost:6081/_health
- **API**: http://localhost:6081/{indexname}/...
- **Container**: `fpindex-server`

### NATS JetStream (Port 4222)
- **Client connections**: localhost:4222
- **Monitoring UI**: http://localhost:8222
- **Container**: `nats-server`

### NATS Box (Management)
- **Interactive shell**: `docker exec -it nats-box sh`
- **Container**: `nats-box`

## Useful Commands

### NATS Management
```bash
# Enter NATS box for management
docker exec -it nats-box sh

# Inside nats-box:
nats stream ls                              # List streams
nats stream info fpindex                    # Show fpindex stream info
nats pub fpindex.test.abc123 '{"h":[1,2,3]}'  # Publish test message
nats stream view fpindex                    # View messages
nats consumer ls fpindex                    # List consumers
```

### fpindex Testing
```bash
# Test basic health
curl http://localhost:6081/_health

# Create an index
curl -X PUT http://localhost:6081/myindex

# Insert a fingerprint
curl -X PUT http://localhost:6081/myindex/12345 \
  -H "Content-Type: application/json" \
  -d '{"hashes": [100, 200, 300]}'

# Search
curl -X POST http://localhost:6081/myindex/_search \
  -H "Content-Type: application/json" \
  -d '{"query": [100, 200], "limit": 10}'

# Bulk update
curl -X POST http://localhost:6081/myindex/_update \
  -H "Content-Type: application/json" \
  -d '{
    "changes": [
      {"insert": {"id": 67890, "hashes": [400, 500, 600]}},
      {"delete": {"id": 12345}}
    ]
  }'
```

### Logs and Debugging
```bash
# View logs
docker-compose logs fpindex
docker-compose logs nats

# Follow logs
docker-compose logs -f fpindex

# Check container health
docker-compose ps
```

## Development Workflow

### Testing the Proxy Service

1. **Start the environment**:
   ```bash
   docker-compose up -d
   ./dev/setup-nats-streams.sh
   ```

2. **Verify fpindex is working**:
   ```bash
   python dev/test-fpindex-api.py
   ```

3. **Implement proxy service** in `src/proxy_service.py`

4. **Test proxy service** by pointing it at localhost:4222 (NATS) and localhost:8080 (fpindex)

### Testing the Sidecar Consumer

1. **Publish test messages to NATS**:
   ```bash
   docker exec nats-server nats pub fpindex.myindex.abc123 '{"h":[100,200,300]}'
   ```

2. **Implement sidecar consumer** in `src/sidecar_consumer.py`

3. **Run sidecar** and verify it updates fpindex

### Integration Testing

1. **Run proxy service** (writes to NATS)
2. **Run sidecar consumer** (reads from NATS, updates fpindex)
3. **Send API requests** to proxy service
4. **Verify updates** appear in fpindex

## Environment Variables

Set these in your development environment:

```bash
# NATS connection
export NATS_URL=nats://localhost:4222

# fpindex connection  
export FPINDEX_URL=http://localhost:6081

# Logging
export LOG_LEVEL=DEBUG
```

## Troubleshooting

### NATS Issues
```bash
# Check NATS health
docker exec nats-server nats server ping

# Check JetStream is enabled
docker exec nats-server nats server info

# View NATS config
docker exec nats-server cat /etc/nats/nats-server.conf
```

### fpindex Issues
```bash
# Check fpindex health
curl http://localhost:6081/_health

# View fpindex logs
docker-compose logs fpindex

# Check if data persists
docker volume inspect acoustid-index_fpindex_data --format '{{.Mountpoint}}' | xargs ls -la
```

### Docker Issues
```bash
# Clean restart
docker-compose down -v
docker-compose up -d --build

# View all containers
docker ps -a

# Clean up volumes (WARNING: deletes all data)
docker-compose down -v
docker volume prune
```

## Performance Testing

Use the benchmark script to test performance:

```bash
# Run existing benchmarks against fpindex
python benchmark.py --num-docs 1000 --num-searches 100

# Test proxy service performance (when implemented)
python benchmark.py --proxy-url http://localhost:8081
```

## Next Steps

1. Implement HTTP proxy service
2. Implement sidecar consumer  
3. Add integration tests
4. Add performance benchmarks for the new architecture
5. Test failure scenarios (NATS down, fpindex down, etc.)