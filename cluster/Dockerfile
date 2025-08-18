# Multi-stage build for fpindex-cluster
FROM python:3.12-slim AS builder

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set uv environment variables for optimal caching and compilation
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# Copy dependency files first for better layer caching
COPY uv.lock pyproject.toml ./

# Install dependencies (excluding the project itself)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Copy application source code
COPY fpindexcluster/ ./fpindexcluster/
COPY README.md ./

# Install the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Production stage - minimal runtime image
FROM python:3.12-slim AS runtime

# Create non-root user for security
RUN groupadd -r fpindex && useradd -r -g fpindex fpindex

# Copy the virtual environment and application from builder stage
COPY --from=builder /app /app

# Set up environment to use the virtual environment
ENV PATH="/app/.venv/bin:$PATH" \
    VIRTUAL_ENV="/app/.venv"

WORKDIR /app

# Change ownership to non-root user
RUN chown -R fpindex:fpindex /app

# Switch to non-root user
USER fpindex

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD fpindex-cluster --version || exit 1

# Default command
ENTRYPOINT ["fpindex-cluster"]
CMD ["--help"]