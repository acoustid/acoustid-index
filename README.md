AcoustID Index
==============

Acoustid Index is a "number search engine". It's similar to text search
engines, but instead of searching in documents that consist of words,
it searches in documents that consist of 32-bit integers.

It's a simple inverted index data structure that doesn't do any kind of
processing on the indexed documents. This is useful for searching in
[Chromaprint][1] audio fingerprints, which are nothing more than 32-bit
integer arrays.

[1]: http://acoustid.org/chromaprint

## Running

Starting server using Docker:

    $ docker run -ti -p 6080 ghcr.io/acoustid/acoustid-index

Starting server locally:

    $ ./fpi-server
    Listening on "127.0.0.1" port 6080

## Building

### Dependencies

 - C/C++ compiler supporting at least C++17
 - CMake
 - Qt6, at least the QtCore, QtNetwork and QtConcurrent components
 - SQLite3
 - GoogleTest (optional)

#### For Ubuntu/Debian

    apt install gcc g++ cmake qt6-base-dev libgtest-dev libsqlite3-dev libprotobuf-dev libgrpc++-dev protobuf-compiler protobuf-compiler-grpc

### Building the code

    cmake .
    cmake --build .

## Usage

### REST API

The current version of the REST API is limited to adding documents to the index and searching. This because the internal index structures do not support document updates, the index is effectively append-only. In the next major version of acoustid-index, the API will be extended to support updates as well.

Also, the API is designed to support multiple indices. However, in the current version it's limited to just one index, named "main". Multi-index support will be added later as well.

#### Add document API

Endpoints:

    PUT /<index>/_doc/<id>

Body fields:

 - `terms`: array of 32-bit integers representing the fingerprint to index

Example HTTP request:

    PUT /main/_doc/1 HTTP/1.1
    Content-Type: application/json

    {"terms":[100,200,300]}
 
Example HTTP response:
 
    HTTP/1.1 200 OK
    Content-Type: application/json
    
    {}
 
#### Search API

Endpoints:

    GET /<index>/_search
    
Query parameters:

   - `query` - comma-separated list of 32-bit numbers representing the fingerprint to search for
   - `limit` - maximum number of results returned, defaults to 100

Example HTTP request:

    GET /main/_search?query=100,200,300&limit=10 HTTP/1.1
 
Example HTTP response:
 
    HTTP/1.1 200 OK
    Content-Type: application/json
    
    {"results":[{"id":1,"score":3}]}

#### Bulk document update API

Endpoints:

    POST /<index>/_bulk

Example HTTP request:

    POST /main/_bulk HTTP/1.1
    Content-Type: application/json
    
    [
      {"upsert": {"id":1, "terms":[100,200,300]}},
      {"upsert": {"id":2, "terms":[500,600,700]}},
    ]

Example HTTP response:
 
    HTTP/1.1 200 OK
    Content-Type: application/json
    
    {}

### Telnet API (legacy)

Example session:

    $ telnet 127.0.0.1 6080
    Trying 127.0.0.1...
    Connected to 127.0.0.1.
    Escape character is '^]'.
    begin
    OK
    insert 1 368308215,364034037,397576085,397509509,393249669,389054869
    OK
    insert 2 1574172159,1598222797,1564660173,1564656069,1564537317,1565584741
    OK
    insert 3 1130316157,1096749341,1075786015,1075655999,1075656047,1079977343
    OK
    commit
    OK
    search 1130316157,397509509,393249669,389054869
    OK 1:3 3:1
    quit
    OK
    Connection closed by foreign host.
