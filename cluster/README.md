# Clustering for the AcoustID index

We are mimicking the HTTP API of fpindex, but instead of actually indexing anything, we are writing
messages to NATS JetStream. These messages are stored permanently and can be replayed to any number
of fpindex instances.

Consists of two components:
- manager (HTTP API for writing to NATS JetStream, it does not proxy to fpindex, search and fingeprint APIs are not implemented)
- updater (reading from NATS JetStream, applying the changes to a single fpindex instance)

NATS structure:
 - subject names: fpindex.$INDEX.$ID
 - stream names: fpindex.$INDEX
 - streams are configured to store max 1 message per subject
 - inserts/updates are msgpack-encoded, maps e.g. `{'h': [1,2,3]}`
 - deletes are written as empty messages (zero bytes)

The manager is responsible for creating NATS JS streams:
 - index exists if there is a stream with the corresponding name
 - index creation is explicit
 - when submitting changes, they get translated to NATS messages
 - when index is deleted, we delete the streams

For each fpindex instance:
 - we start an updater
 - it loads up all index names by listing streams in NATS JS
 - it creates a new permanent consumer for each index
 - loads data into fpindex, max 10000 messages per once, max delay 1s
 - it also subscribes to internal events, to see if new streams are being added or deletes

Technical details:
 - use asyncio, aiohttp, msgspec
 - HTTP API can only support msgpack (unlike the fpindex API, which also supports JSON)
