"""Main entry point for cluster service"""

import asyncio
import logging

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
def proxy(host, port):
    """Run HTTP proxy service"""
    config = Config.from_env()

    # Override with command line arguments
    if host:
        config.proxy_host = host
    if port:
        config.proxy_port = port

    async def run_proxy():
        async with ProxyService(config) as svc:
            logger.info("Service initialized. Starting...")
            await svc.run()

    logger.info(f"Starting proxy service on {config.proxy_host}:{config.proxy_port}")
    asyncio.run(run_proxy())


@cli.command()
@click.option("--consumer-name", default=None, help="Consumer name")
def updater(consumer_name):
    """Run updater service"""
    config = Config.from_env()

    if consumer_name:
        config.consumer_name = consumer_name

    async def run_updater():
        async with UpdaterService(config) as svc:
            logger.info("Service initialized. Starting...")
            await svc.run()

    logger.info(f"Starting updater service with consumer: {config.consumer_name}")
    asyncio.run(run_updater())


if __name__ == "__main__":
    cli()
