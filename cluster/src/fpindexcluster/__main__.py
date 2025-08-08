"""Main entry point for cluster service"""

import asyncio
import logging
import signal
from typing import Protocol

import click

from .config import Config
from .proxy_service import ProxyService
from .updater_service import UpdaterService

logger = logging.getLogger(__name__)


def setup_logging(level: str):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


class Service(Protocol):
    """Base class for services"""

    async def start(self) -> None:
        """Start the service"""
        ...

    async def stop(self) -> None:
        """Stop the service"""
        ...


class ServiceManager:
    """Manages service lifecycle"""

    def __init__(self, service: Service) -> None:
        self.service = service
        self.shutdown_event = asyncio.Event()

    def signal_handler(self):
        """Handle shutdown signals"""
        logger.info("Received shutdown signal")
        self.shutdown_event.set()

    async def run(self):
        loop = asyncio.get_event_loop()
        for sig in [signal.SIGTERM, signal.SIGINT]:
            loop.add_signal_handler(sig, self.signal_handler)

        try:
            await self.service.start()
            logger.info("Service running. Press Ctrl+C to stop.")
            await self.shutdown_event.wait()
        finally:
            await self.service.stop()


@click.group()
@click.option("--log-level", default="INFO", help="Log level")
@click.pass_context
def cli(ctx: click.Context, log_level: str) -> None:
    """fpindex cluster service"""
    ctx.ensure_object(dict)
    ctx.obj["log_level"] = log_level
    setup_logging(log_level)


@cli.command()
@click.option("--host", default=None, help="Proxy host")
@click.option("--port", default=None, type=int, help="Proxy port")
@click.pass_context
def proxy(ctx, host, port):
    """Run HTTP proxy service"""
    config = Config.from_env()

    # Override with command line arguments
    if host:
        config.proxy_host = host
    if port:
        config.proxy_port = port

    service = ProxyService(config)

    manager = ServiceManager(service)
    logger.info(f"Starting proxy service on {config.proxy_host}:{config.proxy_port}")

    asyncio.run(manager.run())


@cli.command()
@click.option("--consumer-name", default=None, help="Consumer name")
@click.pass_context
def updater(ctx, consumer_name):
    """Run updater service"""
    config = Config.from_env()

    if consumer_name:
        config.consumer_name = consumer_name

    service = UpdaterService(config)

    manager = ServiceManager(service)
    logger.info(f"Starting updater service with consumer: {config.consumer_name}")

    asyncio.run(manager.run())


if __name__ == "__main__":
    cli()
