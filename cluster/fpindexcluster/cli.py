#!/usr/bin/env python3

import argparse
import asyncio
import logging
import signal
import sys
import socket

import nats

from .server import start_server
from .manager import IndexManager
from contextlib import AsyncExitStack


async def main_async(args):
    # Set up logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    # Create shutdown event
    shutdown_event = asyncio.Event()

    def signal_handler():
        logger.info("Received shutdown signal")
        shutdown_event.set()

    # Register signal handlers using asyncio
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    async with AsyncExitStack() as stack:
        # Connect to NATS
        logger.info(f"Connecting to NATS server at {args.nats_url}")
        nc = await nats.connect(args.nats_url)
        stack.push_async_callback(nc.close)

        # Set up index manager and JetStream
        logger.info("Setting up index manager")
        manager = await IndexManager.create(nc, args.nats_prefix, args.fpindex_url, args.instance)

        # Register manager cleanup
        stack.push_async_callback(manager.cleanup)

        # Start HTTP server
        logger.info(f"Starting HTTP server on {args.listen_host}:{args.listen_port}")
        server = await start_server(nc, manager, args.listen_host, args.listen_port)
        stack.push_async_callback(server.cleanup)

        # Keep the server running until shutdown signal
        logger.info("Server is running. Send SIGTERM or SIGINT to stop.")
        await shutdown_event.wait()

    logger.info("Server stopped")
    return 0


def main():
    parser = argparse.ArgumentParser(
        prog="fpindex-cluster",
        description="Fingerprint index cluster management tool",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    hostname = socket.gethostname()

    parser.add_argument("--version", action="version", version="%(prog)s 0.1.0")

    parser.add_argument(
        "--nats-url",
        metavar="URL",
        default="nats://localhost:4222",
        help="NATS server URL",
    )

    parser.add_argument(
        "--nats-prefix",
        metavar="PREFIX",
        default="fpindex",
        help="NATS subject prefix",
    )

    parser.add_argument(
        "--fpindex-url",
        metavar="URL",
        default="http://localhost:6081",
        help="Base URL for fpindex instance",
    )

    parser.add_argument(
        "--listen-host",
        metavar="HOST",
        default="0.0.0.0",
        help="HTTP server host",
    )

    parser.add_argument(
        "--listen-port",
        metavar="PORT",
        type=int,
        default=8081,
        help="HTTP server port",
    )

    parser.add_argument(
        "--log-level",
        metavar="LEVEL",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level",
    )

    parser.add_argument(
        "--instance",
        metavar="NAME",
        default=hostname,
        help="Instance name for this cluster node",
    )

    args = parser.parse_args()

    try:
        return asyncio.run(main_async(args))
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
