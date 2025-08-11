FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy cluster service
COPY . ./cluster/

# Create non-root user
RUN useradd -m -s /bin/bash -u 6081 acoustid

# Set Python path so we can import cluster module
ENV PYTHONPATH=/app/src

# Switch to non-root user
USER acoustid

# Health check
HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD wget --quiet --tries=1 --spider http://localhost:8080/_health || exit 1

# Default command (can be overridden)
CMD ["python", "-m", "fpindexcluster", "proxy"]