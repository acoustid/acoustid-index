import asyncio
import aiohttp
import msgpack
import argparse
import traceback


class ProtocolError(Exception):

    def __init__(self, msg):
        self.msg = msg


class Protocol:

    def __init__(self, session):
        self.session = session
        self.changes = []

    async def search(self, query):
        url = self.index_url + f"/{self.index_name}/_search"
        data = msgpack.dumps({"q": query})
        headers = {
            "Content-Type": "application/vnd.msgpack",
            "Accept": "application/vnd.msgpack",
        }
        async with self.session.post(url, data=data, headers=headers) as resp:
            resp.raise_for_status()
            body = msgpack.loads(await resp.content.read())
            return [(r["i"], r["s"]) for r in body["r"]]

    async def update(self, changes):
        print(f'sending update with {len(changes)} changes')
        url = self.index_url + f"/{self.index_name}/_update"
        data = msgpack.dumps({"c": changes})
        headers = {
            "Content-Type": "application/vnd.msgpack",
            "Accept": "application/vnd.msgpack",
        }
        async with self.session.post(url, data=data, headers=headers) as resp:
            body = await resp.content.read()
            resp.raise_for_status()

    async def get_attribute(self, name):
        url = self.index_url + f"/{self.index_name}"
        headers = {
            "Content-Type": "application/vnd.msgpack",
            "Accept": "application/vnd.msgpack",
        }
        async with self.session.get(url, headers=headers) as resp:
            resp.raise_for_status()
            body = msgpack.loads(await resp.content.read())
            return body["a"].get(name, 0)

    async def set_attribute(self, name, value):
        changes = [{"s": {"n": name, "v": value}}]
        await self.update(changes)

    async def handle_request(self, request):
        if not request:
            raise ProtocolError("invalid command")

        if request[0] == "search":
            query = list(map(int, request[1].split(",")))
            results = await self.search(query)
            return " ".join(f"{docid}:{hits}" for (docid, hits) in results)

        if request[0] == "begin":
            self.changes = []
            return ""

        if request[0] == "rollback":
            self.changes = []
            return ""

        if request[0] == "commit":
            await self.update(self.changes)
            self.changes = []
            return ""

        if request[0] == "insert":
            self.changes.append(
                {
                    "i": {
                        "i": int(request[1]),
                        "h": [int(v)&0xffffffff for v in request[2].split(",")],
                    }
                }
            )
            return ""

        if request[0] == "get":
            if len(request) == 3 and request[1] == "attribute":
                value = await self.get_attribute(request[2])
                return str(value)
            elif len(request) == 2:
                value = await self.get_attribute(request[1])
                return str(value)

        if request[0] == "set":
            if len(request) == 4 and request[1] == "attribute":
                await self.set_attribute(request[2], int(request[3]))
                return ""
            elif len(request) == 3:
                await self.set_attribute(request[1], int(request[2]))
                return ""

        raise ProtocolError("invalid command")


class Server:

    def __init__(self, target):
        self.index_name = "main"
        self.index_url = target

    async def create_index(self):
        url = self.index_url + f"/{self.index_name}"
        async with self.session.put(url) as resp:
            resp.raise_for_status()

    async def serve(self, listen_host, listen_port):
        async with aiohttp.ClientSession() as session:
            self.session = session
            await self.create_index()
            server = await asyncio.start_server(
                self.handle_connection, listen_host, listen_port
            )
            async with server:
                await server.serve_forever()

    async def handle_connection(self, reader, writer):
        try:
            proto = Protocol(self.session)
            proto.index_name = self.index_name
            proto.index_url = self.index_url

            while True:
                try:
                    line = await reader.readuntil(b"\n")
                except asyncio.exceptions.IncompleteReadError:
                    return

                try:
                    response = await proto.handle_request(line.decode("ascii").split())
                    writer.write(b"OK " + response.encode("ascii") + b"\n")
                except ProtocolError as ex:
                    writer.write(b"ERR " + ex.msg.encode("ascii") + b"\n")
                except Exception:
                    traceback.print_exc()
                    writer.write(b"ERR internal error\n")

                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", default=6080, type=int)
    parser.add_argument("--target", default="http://127.0.0.1:6081")
    args = parser.parse_args()
    srv = Server(target=args.target)
    await srv.serve(args.listen_host, args.listen_port)


if __name__ == "__main__":
    asyncio.run(main())
