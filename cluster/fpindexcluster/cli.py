#!/usr/bin/env python3

import argparse
import asyncio
import sys

import nats


async def connect_to_nats(url: str) -> nats.NATS:
    nc = await nats.connect(url)
    print(f"Connected to NATS server at {url}")
    return nc


async def main_async(args):
    nc = await connect_to_nats(args.nats_url)
    
    print("fpindex-cluster: Connected to NATS, but not implemented yet")
    
    await nc.close()
    return 0


def main():
    parser = argparse.ArgumentParser(
        prog="fpindex-cluster",
        description="Fingerprint index cluster management tool",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0"
    )
    
    parser.add_argument(
        "--nats-url",
        metavar="URL",
        default="nats://localhost:4222",
        help="NATS server URL"
    )
    
    parser.add_argument(
        "--nats-prefix",
        metavar="PREFIX",
        default="fpindex",
        help="NATS subject prefix"
    )
    
    parser.add_argument(
        "--fpindex-url",
        metavar="URL",
        default="http://localhost:8080",
        help="Base URL for fpindex instance"
    )
    
    args = parser.parse_args()
    
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())