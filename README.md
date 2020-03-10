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

Reindexing from the Acoustid database:

    $ echo "COPY (SELECT id, acoustid_extract_query(fingerprint) FROM fingerprint) TO stdout DELIMITER '|' " \
           | psql -U acoustid acoustid | ./fpi-import -d idx/ -c -o
Running:

    $ ./fpi-server
    Listening on "127.0.0.1" port 6080

Using Docker:

    $ docker run -ti -p 6080 quay.io/acoustid/acoustid-index
    $ docker run -ti quay.io/acoustid/acoustid-index fpi-import -d /var/lib/acoustid-index/ -c -o

Simple client session:

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
