FROM ubuntu:24.04

# wget is here for container healthchecks (GET /_health); nothing in the server
# needs it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -s /bin/bash -u 6081 acoustid

RUN mkdir -p /var/lib/fpindex && chown acoustid /var/lib/fpindex
VOLUME ["/var/lib/fpindex"]

# Built outside by `zig build --release=fast`, not in a builder stage, so the
# image is only buildable after that has run. The binary links the runner's
# glibc, which is why CI pins ubuntu-24.04 to match this base.
COPY --chmod=755 zig-out/bin/fpindex /usr/bin/fpindex

USER acoustid
EXPOSE 8080

CMD ["fpindex", "--dir", "/var/lib/fpindex", "--host", "0.0.0.0", "--port", "8080"]
