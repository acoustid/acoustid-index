FROM ubuntu:xenial

RUN useradd -ms /bin/bash acoustid

RUN apt-get update && \
    apt-get install -y libqt4-network libqtcore4 libstdc++6 libgcc1

ADD acoustid-index_*.deb /tmp/
RUN dpkg -i /tmp/acoustid-index_*.deb && rm /tmp/acoustid-index_*.deb

RUN mkdir -p /var/lib/acoustid-index && chown -R acoustid /var/lib/acoustid-index
VOLUME ["/var/lib/acoustid-index"]

ADD docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]

USER acoustid
EXPOSE 6080

CMD ["fpi-server", "-d", "/var/lib/acoustid-index", "-a", "0.0.0.0", "-p", "6080", "-m"]
