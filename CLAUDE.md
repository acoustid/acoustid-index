# CLAUDE.md

## Overview

* This component is a part of the AcoustID project
* Inverted index for searching in audio fingerprints (ID + set of hashes)
* Search is defined as finding fingerprint IDs that have non-empty intersection with the query (set of hashes)
* Running as a service, accessed by other components via HTTP API using compact MessagePack-encoded messages
* Fingerprints can be added, updated and deleted from the index in real time using the HTTP API
* Fingerprints are immediately available for search using the HTTP API

## Goals

* Very fast search using minimal resources (needs to be cost effective to run)
* Index updates can be slower, but happening in the real-time, not done in batches
* Scaling
   - 100 million fingerprints, 150 hashes in each fingerprint
   - search results delivered below 50ms, ideally much lower
   - does not use more than 64GB of memory

## Architecture

General:

* Data structure similar to LSM trees
* Index consists of a list of segments
* Segment is essentially a list of sorted (hash, id) pairs
* Updates are added to the index as new segments
* Older segments are being merged in the background to keep the total list of segments constrained
* Search iterates over segments in the index, uses binary search to find matching hashes within each segment, collects results, sorts them

How data is stored:

* When new update comes, it's first saved into a WAL file (`src/Oplog.zig`)
* If WAL operation succeeds, we create a new in-memory index with very simple internal structure and append it to the index (`src/MemorySegment.zig`)
* If we reach a certain number of segments, we select multiple segments and merge them into a large one, optimizing for a nice distribution of active segment sizes (`src/segment_merger.zig`, `src/segment_merge_policy.zig`)
* When an in-memory segments gets too large, it gets converted into a file that has more complex structure with fixed-size compressed blocks of (hash, id) pairs (`src/FileSegment.zig`, `src/filefmt.zig`)
* Blocks use StreamVByte compression for (hash, id) pairs with SIMD accelerated decoding (`src/streamvbyte.zig`, `src/streamvbyte_*.c`)
* File segments have list of the first hash for each blocks in heap-allocated memory, but the actually block data are memory mapped
* When searching in a file segment, we first use binary search over an index of blocks, then decompress matching blocks and search within each block

Other:

* Multiple named indexes can be managed via the service (`src/MultiIndex.zig`)

## Build and Test Commands

Build the executable:

    zig build

Run unit tests:

    zig build unit-tests --summary all

Run integration tests:

    zig build e2e-tests --summary all

Test runner supports environment variables:
- `TEST_VERBOSE=false` - Disable verbose output (default: true)
- `TEST_FAIL_FIRST=true` - Stop on first failure  
- `TEST_FILTER=substring` - Filter tests by name

You can also run integration tests directly:

    python3 -m venv venv
    source venv/bin/activate
    pip install -r tests/requirements.txt
    pytest -vv tests/

## HTTP API

The server provides REST endpoints for:
- Index management: `PUT/GET/DELETE /:indexname`
- Fingerprint operations: `POST /:indexname/_search`, `POST /:indexname/_update`
- Single fingerprint: `PUT/GET/DELETE /:indexname/:fpid`
- System: `GET /_health`, `GET /:indexname/_health`, `GET /_metrics`

Primarily uses MessagePack with compact message structure (using single character as map keys), but allows JSON for human interactions.

## Development Notes

- Don't break backwards compatibility unless absolutely necessary
- Run unit tests after any code change
- Run integration tests, and ask code-reviewer agent for code review, after finishing any larger code change
- Ask architect agent for review when planning a change to the system

## Agent Instructions

- Talk like a pragmatic programmer, don't be overly excited, don't be overly formal, this is not an enterprise project
