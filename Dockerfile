FROM ubuntu:24.04

RUN useradd -m -s /bin/bash -u 1000 acoustid

ADD zig-out/bin/fpindex /usr/bin

RUN mkdir -p /var/lib/acoustid/fpindex && chown -R acoustid /var/lib/acoustid/fpindex
VOLUME ["/var/lib/acoustid/fpindex"]

USER acoustid
EXPOSE 6080

CMD ["fpindex", "--dir", "/var/lib/acoustid/fpindex", "--address", "0.0.0.0", "--port", "6080"]