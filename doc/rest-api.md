# REST APIs

## Index APIs

### Create index API

Creates a new index.

#### Request

    PUT /<index>

### Get index API

Returns information about an index.

#### Request

    GET /<index>

### Index exists API

Check if an index exists.

#### Request

    HEAD /<index>

### Delete index API

Deletes an index.

#### Request

    DELETE /<index>

## Search APIs

### Search API

### Request

    GET /_search
    GET /<index>/_search

## Healthchecks

### Liveness check

Check if the server is running, to be used in Kubernetes `livenessProbe`.

#### Request

    GET /_health/alive

### Readiness check

Check if the server is ready to serve requests, to be used in Kubernetes `readinessProbe`.

#### Request

    GET /_health/ready

## Metrics

### Prometheus metrics endpoint

Returns metrics in Prometheus format

#### Request

    GET /_metrics
