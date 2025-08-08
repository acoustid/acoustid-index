# Fingerprint Index Cluster Service

HTTP proxy service for the AcoustID fingerprint index cluster, built with NATS JetStream integration.

## Development

### Setup

Install dependencies using uv:

```bash
uv pip install -e .
```

For development with testing dependencies:

```bash
uv pip install -e .[dev]
```

Or use the generated requirements.txt:

```bash
pip install -r requirements.txt
```

### Running

```bash
python -m fpindexcluster proxy
```

### Testing

Run tests with pytest:

```bash
pytest
```

### Docker

Build and run with Docker:

```bash
docker build -t fpindexcluster .
docker run -p 8080:8080 fpindexcluster
```

## Dependencies

Dependencies are managed with uv. To update requirements.txt:

```bash
uv pip compile pyproject.toml -o requirements.txt
```