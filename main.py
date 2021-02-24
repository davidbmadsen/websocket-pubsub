import threading
from datetime import datetime
import time
import json
import ssl
import websockets
import asyncio
from config import Config


class WebsocketPubSub(threading.Thread):

    """
    Class that hosts WebSocket pub/sub.

    Default port for connecting clients is 64000
    """

    def __init__(self, port=64000):
        threading.Thread.__init__(self)
        self._stop_event = threading.Event()
        self.daemon = True
        self.addr = None
        self.port = port
        self.loop = asyncio.new_event_loop()
        self.queue = asyncio.Queue(loop=self.loop)
        self.subs = set()
        self.ws_type = 'wss' if Config.USE_SSL else 'ws'

    def stop(self):
        try:
            asyncio.get_event_loop().stop()
        except Exception as e:
            raise e
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def run(self):

        asyncio.set_event_loop(self.loop)

        if self.ws_type == 'wss':
            print("WebSocket server (Secure) started for %s:%s at %s" %
                  (self.addr, self.port, datetime.now()))
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ssl_context.load_cert_chain(
                Config.SSL_CERT_CHAIN, Config.SSL_CERT_KEY
            )
            start_server = websockets.serve(
                self.server_handler, None, self.port, ssl=ssl_context)
        else:
            print("WebSocket server started for %s:%s at %s" %
                  (self.addr, self.port, datetime.now()))
            start_server = websockets.serve(
                self.server_handler, None, self.port)
        async_worker = asyncio.ensure_future(self.async_worker())

        self.loop.run_until_complete(
            asyncio.gather(
                start_server,
                async_worker
            )
        )

        asyncio.get_event_loop().run_forever()

    async def register(self, websocket):
        self.subs.add(websocket)
        print("New connection. Connections: %s" % len(self.subs))

    async def unregister(self, websocket):
        self.subs.remove(websocket)
        print("Unregistered websocket. Connections: %s" % len(self.subs))

    async def server_handler(self, websocket, path):
        """Websocket server handler coroutine. Routes messages based on path variable.

        Args:
            websocket (Websocket): websocket connection instance
            path (string): the message path for routing, i.e. "/pub"
        """

        print("Handler started for path '%s'" % path)
        global n
        n = 0

        if path == "/sub":
            n += 1
            i = n
            await self.register(websocket)
            print("[%s] Adding subscriber. Subscribers: %s. " %
                  (self.name, len(self.subs)))
            try:
                async for msg in websocket:
                    print(msg)
                    pass

            except websockets.ConnectionClosed:
                print("[%s] Connection closed" % self.name)
                pass

            finally:
                await self.unregister(websocket)
                print("[%s] Total subs connected: %s " %
                      (self.name, len(self.subs)))

        elif path == "/pub":
            print("[%s] Publisher connected" % self.name)
            t_0 = time.time()
            while not self.stopped():
                try:
                    async for msg in websocket:
                        print("[%s] Echo: %s" % (self.name, msg))
                        if msg == 'ping':
                            print("[%s] [%s] Ping successful" %
                                  (self.name, datetime.now()))
                            continue
                        for ws in self.subs:
                            asyncio.ensure_future(ws.send(msg))

                except websockets.exceptions.ConnectionClosedError:
                    t_1 = time.time()
                    print("Publisher disconnected (%ss)." %
                          (round(t_1 - t_0, 2)))
                    break

    # --- Websocket server things --- #
    async def ping(self, websocket, interval):
        """Ping coroutine for keeping the publisher connected while no messages are passed.

        Args:
            websocket (Websocket): the websocket connection instance
            interval (int): Time between ping intervals
        """

        print("[%s] Ping started, interval %s sec." % (self.name, interval))

        while not self.stopped():
            await asyncio.sleep(interval)
            await websocket.send('ping')

        print("[%s] Ping stopped (%s)" % (self.name, datetime.now()))

    @staticmethod
    async def send_data(websocket, queue):
        while True:
            try:
                data = await queue.get()
                if data:
                    await websocket.send(json.dumps(data))

            except websockets.exceptions.ConnectionClosedError as e:
                raise e

    async def async_worker(self):
        """Interface that gathers coroutines for handling all async calls
        """

        print("Async handler started")
        async_queue = asyncio.Queue()
        domain = 'localhost' if not Config.SERVER_PUBLIC else Config.DOMAIN
        publisher_uri = self.ws_type + '://' + \
            domain + ':' + str(self.port) + '/pub'

        if self.ws_type == 'wss':
            async with websockets.connect(publisher_uri, ssl=True) as websocket:
                await asyncio.gather(
                    self.send_data(websocket, async_queue),
                    self.ping(websocket, 30)
                )
        else:
            # Non SSL case (for testing, don't use in production)
            async with websockets.connect(publisher_uri) as websocket:
                await asyncio.gather(
                    self.send_data(websocket, async_queue),
                    self.ping(websocket, 30)
                )
