FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends glibc-tools wget && rm -rf /var/lib/apt/lists/*

RUN useradd -m -s /bin/bash -u 6081 acoustid

ADD zig-out/bin/fpindex /usr/bin

RUN mkdir -p /var/lib/acoustid-index && chown -R acoustid /var/lib/acoustid-index
VOLUME ["/var/lib/acoustid-index"]

USER acoustid
EXPOSE 6081

# Add health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:6081/_health || exit 1

CMD ["fpindex", "--dir", "/var/lib/acoustid-index", "--address", "0.0.0.0", "--port", "6081"]
