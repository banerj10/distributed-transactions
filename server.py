import asyncio
import logging
import pickle
import socket

from messages import *
import nodeslist
from ui import UI


class Storage:
    def __init__(self):
        # key(str) -> DataObj
        self._actual = dict()

        self._buffer = dict()
        self._buffer_txn = dict()

        for client in nodeslist.clients:
            # key(str) -> DataObj
            client_ip = socket.gethostbyname(client)
            self._buffer[client_ip] = dict()
            self._buffer_txn[client_ip] = -1

    def actual(self):
        return self._actual

    def buffer(self, client_ip):
        return self._buffer[client_ip]

    def buffer_txn(self, client_ip):
        return self._buffer_txn[client_ip]

    def set_buffer_txn(self, client_ip, txn_id):
        self._buffer_txn[client_ip] = txn_id

    class DataObj:
        def __init__(self, value, last_rd_txn=-1, last_wr_txn=-1):
            self.value = value
            self.last_rd_txn = last_rd_txn
            self.last_wr_txn = last_wr_txn


class ServerNetwork:
    PORT = 13337
    SELF_ADDR = socket.gethostbyname(socket.gethostname())

    def __init__(self):
        self.evloop = asyncio.get_event_loop()
        self.storage = Storage()

    async def create_server(self):
        self.server = await self.evloop.create_server(
            lambda: ServerProtocol(self.request_handler),
            port=ServerNetwork.PORT, family=socket.AF_INET,
            reuse_address=True, reuse_port=True
        )
        UI.log('Created server...')

    def request_handler(self, msg, transport):
        cls = msg.__class__.__name__
        handler = getattr(self, f'handle_{cls}', None)

        if handler is None:
            UI.log(f'Dont recognize msg {cls}', level=logging.WARNING)
            return

        UI.log(f'Got {str(msg)}')
        response = handler(msg)

        if response is not None:
            response.origin = ServerNetwork.SELF_ADDR
            response.destination = msg.origin

            UI.log(f'Sending {str(response)}')
            pickled = pickle.dumps(response, pickle.HIGHEST_PROTOCOL)
            transport.write(pickled)

    def handle_SetMsg(self, msg):
        client_ip = msg.origin

        if self.storage.buffer_txn(client_ip) > msg.txn_id:
            curr_txn_id = self.storage.buffer_txn(client_ip)
            UI.log(f'!!! TXN ORDERING VIOLATED !!!', level=logging.CRITICAL)
            UI.log(f'RECVD {msg.txn_id} over {curr_txn_id}',
                   level=logging.CRITICAL)

        # update current txn ID for that client
        self.storage.set_buffer_txn(client_ip, msg.txn_id)

        # check key in actual storage
        if msg.key in self.storage.actual():
            last_rd_txn = self.storage.actual()[msg.key].last_rd_txn
            last_wr_txn = self.storage.actual()[msg.key].last_wr_txn

            # check if has permission to write
            if msg.txn_id >= last_rd_txn and msg.txn_id >= last_wr_txn:
                # update write permission
                self.storage.actual()[msg.key].last_wr_txn = msg.txn_id

                # write key to cache (never write to actual in SetMsg!)
                if msg.key in self.storage.buffer(client_ip):
                    self.storage.buffer(client_ip)[msg.key].value = msg.value
                else:
                    self.storage.buffer(client_ip)[msg.key] = \
                        Storage.DataObj(msg.value)
                success = True
            else:
                # NO PERMISSION!
                success = False
        else:
            # key not in storage, add/update to buffer
            if msg.key in self.storage.buffer(client_ip):
                self.storage.buffer(client_ip)[msg.key].value = msg.value
            else:
                self.storage.buffer(client_ip)[msg.key] = \
                    Storage.DataObj(msg.value)
            success = True

        # TODO: handle abort cases when success == False

        response = SetMsgResponse(msg.uid, success)
        return response

    def handle_GetMsg(self, msg):
        client_ip = msg.origin

        if self.storage.buffer_txn(client_ip) > msg.txn_id:
            curr_txn_id = self.storage.buffer_txn(client_ip)
            UI.log(f'!!! TXN ORDERING VIOLATED !!!', level=logging.CRITICAL)
            UI.log(f'RECVD {msg.txn_id} over {curr_txn_id}',
                   level=logging.CRITICAL)

        # update current txn ID for that client
        self.storage.set_buffer_txn(client_ip, msg.txn_id)

        value = ''

        # check key in actual storage
        if msg.key in self.storage.actual():
            last_wr_txn = self.storage.actual()[msg.key].last_wr_txn

            # check if has permission to read
            if msg.txn_id >= last_wr_txn:
                # update read permission
                self.storage.actual()[msg.key].last_rd_txn = msg.txn_id

                # get key from cache or actual storage
                if msg.key in self.storage.buffer(client_ip):
                    value = self.storage.buffer(client_ip)[msg.key].value
                else:
                    value = self.storage.actual()[msg.key].value
                success = True
            else:
                # NO PERMISSION!
                success = False
        else:
            # key not in actual storage, try to get from buffer
            if msg.key in self.storage.buffer(client_ip):
                value = self.storage.buffer(client_ip)[msg.key].value
            # if key not in buffer either,
            # then value will be = '' but success = True
            success = True

        # TODO: handle abort cases when success == False

        response = GetMsgResponse(msg.uid, success, value)
        return response

    def handle_TryCommitMsg(self, msg):
        client_ip = msg.origin

        if self.storage.buffer_txn(client_ip) > msg.txn_id:
            curr_txn_id = self.storage.buffer_txn(client_ip)
            UI.log(f'!!! TXN ORDERING VIOLATED !!!', level=logging.CRITICAL)
            UI.log(f'RECVD {msg.txn_id} over {curr_txn_id}',
                   level=logging.CRITICAL)

        # update current txn ID for that client
        self.storage.set_buffer_txn(client_ip, msg.txn_id)

        success = True

        # try to see if its possible to store each key in the actual store
        for key, data_obj in self.storage.buffer(client_ip):
            if key in self.storage.actual():
                last_rd_txn = self.storage.actual()[key].last_rd_txn
                last_wr_txn = self.storage.actual()[key].last_wr_txn

                # check if DOES NOT have permission to write
                if msg.txn_id < last_rd_txn or msg.txn_id < last_wr_txn:
                    success = False
                    break

        # TODO: handle abort cases when success == False

        response = TryCommitMsgResponse(msg.uid, success)
        return response

    def handle_DoCommitMsg(self, msg):
        client_ip = msg.origin

        if self.storage.buffer_txn(client_ip) > msg.txn_id:
            curr_txn_id = self.storage.buffer_txn(client_ip)
            UI.log(f'!!! TXN ORDERING VIOLATED !!!', level=logging.CRITICAL)
            UI.log(f'RECVD {msg.txn_id} over {curr_txn_id}',
                   level=logging.CRITICAL)

        # update current txn ID for that client
        self.storage.set_buffer_txn(client_ip, -1)

        for key, data_obj in self.storage.buffer(client_ip):
            data = Storage.DataObj(data_obj.value, last_wr_txn=msg.txn_id)
            self.storage.actual()[key] = data

        # clear the buffer
        self.storage.buffer(client_ip).clear()

    def close(self):
        self.server.close()


class ServerProtocol(asyncio.Protocol):
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
        UI.log(f'Got data from {self.peer}')
        unpickled = pickle.loads(data)
        self.req_handler(unpickled, self.transport)

    def eof_received(self):
        pass


def main():
    logging.basicConfig(filename='server.log', level=logging.DEBUG)
    UI.log('===========================================')
    UI.log('==== Distributed Transactions - Server ====')
    UI.log('===========================================')

    evloop = asyncio.get_event_loop()
    evloop.set_debug(True)

    server_network = ServerNetwork()
    main_task = evloop.create_task(server_network.create_server())
    try:
        evloop.run_forever()
    except KeyboardInterrupt:
        server_network.close()
        UI.log('BYE!')


if __name__ == '__main__':
    main()
