FROM python:3

RUN pip install --no-cache-dir aiohttp

ADD proxy.py /usr/src/fpindex-proxy.py

CMD [ "python", "/usr/src/fpindex-proxy.py" ]
