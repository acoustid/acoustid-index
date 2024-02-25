FROM ubuntu:22.04

RUN useradd -m -s /bin/bash -u 1000 acoustid

RUN apt-get update && \
    apt-get install -y libqt6network6 libqt6core6 libstdc++6 libgcc1 libgcc-s1

ADD acoustid-index.deb /tmp/
RUN dpkg -i /tmp/acoustid-index.deb && rm /tmp/acoustid-index.deb

RUN mkdir -p /var/lib/acoustid-index && chown -R acoustid /var/lib/acoustid-index
VOLUME ["/var/lib/acoustid-index"]

RUN apt-get update && \
    apt-get install -y rsync

USER acoustid
EXPOSE 6080

CMD ["fpi-server", "--directory", "/var/lib/acoustid-index", "--mmap", "--address", "0.0.0.0", "--port", "6080", "--http-address", "0.0.0.0", "--http-port", "6081"]
