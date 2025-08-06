"""Main entry point for cluster service"""

import asyncio
import logging
import signal
from typing import Optional

import click

from .config import Config
from .proxy_service import ProxyService


def setup_logging(level: str):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )


class ServiceManager:
    """Manages service lifecycle"""
    
    def __init__(self):
        self.service: Optional[ProxyService] = None
        self.shutdown_event = asyncio.Event()
    
    def signal_handler(self):
        """Handle shutdown signals"""
        logging.info("Received shutdown signal")
        self.shutdown_event.set()
    
    async def run_proxy(self, config: Config):
        """Run proxy service"""
        self.service = ProxyService(config)
        
        try:
            await self.service.start()
            logging.info("Proxy service running. Press Ctrl+C to stop.")
            await self.shutdown_event.wait()
        finally:
            if self.service:
                await self.service.stop()
    
    async def run_updater(self, config: Config):
        """Run updater service (placeholder for milestone 3)"""
        logging.info("Updater service not implemented yet")
        await self.shutdown_event.wait()


@click.group()
@click.option("--log-level", default="INFO", help="Log level")
@click.pass_context
def cli(ctx, log_level):
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
    
    manager = ServiceManager()
    
    # Setup signal handlers
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    for sig in [signal.SIGTERM, signal.SIGINT]:
        loop.add_signal_handler(sig, manager.signal_handler)
    
    try:
        loop.run_until_complete(manager.run_proxy(config))
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


@cli.command()
@click.option("--consumer-name", default=None, help="Consumer name")
@click.pass_context
def updater(ctx, consumer_name):
    """Run updater service (not implemented yet)"""
    config = Config.from_env()
    
    if consumer_name:
        config.consumer_name = consumer_name
    
    manager = ServiceManager()
    
    # Setup signal handlers
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    for sig in [signal.SIGTERM, signal.SIGINT]:
        loop.add_signal_handler(sig, manager.signal_handler)
    
    try:
        loop.run_until_complete(manager.run_updater(config))
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    cli()