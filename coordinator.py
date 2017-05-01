import asyncio
import logging
import pickle
import socket

from messages import NewTxnID
from ui import UI


class CoordinatorNetwork:
    PORT = 13337
    SELF_ADDR = socket.gethostbyname(socket.gethostname())

    def __init__(self):
        self.evloop = asyncio.get_event_loop()
        self.last_txn_id = 0

    async def create_server(self):
        self.server = await self.evloop.create_server(
            lambda: CoordinatorProtocol(self.request_handler),
            port=CoordinatorNetwork.PORT, family=socket.AF_INET,
            reuse_address=True, reuse_port=True
        )
        logging.info('Created server...')

    def request_handler(self, msg, transport):
        cls = msg.__class__.__name__
        handler = getattr(self, f'handle_{cls}', None)

        if handler is None:
            UI.log(f'Dont recognize msg {cls}', level=logging.WARNING)
            return

        UI.log(f'Got {cls}')
        response = handler(msg)

        if response is not None:
            response.origin = CoordinatorNetwork.SELF_ADDR
            response.destination = msg.origin

            UI.log(f'Sending {str(response)}')
            pickled = pickle.dumps(response, pickle.HIGHEST_PROTOCOL)
            transport.write(pickled)

    def handle_RequestTxnID(self, msg):
        self.last_txn_id += 1
        response = NewTxnID(msg.uid, self.last_txn_id)
        return response

    def close(self):
        self.server.close()


class CoordinatorProtocol(asyncio.Protocol):
    def __init__(self, req_handler):
        self.evloop = asyncio.get_event_loop()
        self.req_handler = req_handler
        UI.log('Created protocol!')

    def connection_made(self, transport):
        self.transport = transport
        self.peer = self.transport.get_extra_info('peername')[0]
        UI.log(f'Got connection from {str(self.peer)}')

    def connection_lost(self, exc):
        UI.log(f'Connection lost with {str(self.peer)}')
        super().connection_lost(exc)

    def data_received(self, data):
        logging.info(f'Got data from {self.peer}')
        unpickled = pickle.loads(data)
        self.req_handler(unpickled, self.transport)

    def eof_received(self):
        pass


def main():
    logging.basicConfig(filename='coordinator.log', level=logging.DEBUG)
    UI.log('================================================')
    UI.log('==== Distributed Transactions - Coordinator ====')
    UI.log('================================================')

    evloop = asyncio.get_event_loop()
    evloop.set_debug(True)

    coord_network = CoordinatorNetwork()
    main_task = evloop.create_task(coord_network.create_server())
    try:
        evloop.run_forever()
    except KeyboardInterrupt:
        UI.log('BYE!')


if __name__ == '__main__':
    main()
