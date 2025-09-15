# Base stage with environment setup
FROM ubuntu:24.04 AS base

RUN apt-get update && apt-get install -y glibc-tools wget

RUN useradd -m -s /bin/bash -u 6081 acoustid

RUN mkdir -p /var/lib/acoustid-index && chown -R acoustid /var/lib/acoustid-index
VOLUME ["/var/lib/acoustid-index"]

USER acoustid
EXPOSE 6081

CMD ["fpindex", "--dir", "/var/lib/acoustid-index", "--address", "0.0.0.0", "--port", "6081"]

# Final stage with binary copied in
FROM base AS final

ADD zig-out/bin/fpindex /usr/bin
