"""Configuration management for cluster service"""

import os
from dataclasses import dataclass


@dataclass
class Config:
    """Configuration for cluster service"""

    # NATS configuration
    nats_url: str = "nats://localhost:4222"
    nats_stream_prefix: str = "fpindex"

    # fpindex configuration
    fpindex_url: str = "http://localhost:6081"

    # Proxy configuration
    proxy_host: str = "0.0.0.0"
    proxy_port: int = 8080

    # Updater configuration
    consumer_name: str = "fpindex-updater"

    # Logging
    log_level: str = "INFO"
    
    def get_stream_name(self, index_name: str) -> str:
        """Get stream name for a specific index or control stream"""
        return f"{self.nats_stream_prefix}-{index_name}"

    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables"""
        return cls(
            nats_url=os.getenv("NATS_URL", cls.nats_url),
            nats_stream_prefix=os.getenv("NATS_STREAM_PREFIX", cls.nats_stream_prefix),
            fpindex_url=os.getenv("FPINDEX_URL", cls.fpindex_url),
            proxy_host=os.getenv("PROXY_HOST", cls.proxy_host),
            proxy_port=int(os.getenv("PROXY_PORT", str(cls.proxy_port))),
            consumer_name=os.getenv("CONSUMER_NAME", cls.consumer_name),
            log_level=os.getenv("LOG_LEVEL", cls.log_level),
        )
