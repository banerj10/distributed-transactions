import asyncio
import logging
import pickle
import socket

from messages import *
import nodeslist
from ui import UI


class ClientNetwork:
    PORT = 13337
    SELF_ADDR = socket.gethostbyname(socket.gethostname())

    def __init__(self):
        self.evloop = asyncio.get_event_loop()

        # uuid4 -> (asyncio.Event, Response msg)
        self._events = dict()

        # server_name -> ClientNetwork.Peer
        self._servers = dict()
        self.coordinator = None  # type: ClientNetwork.Peer

    def events(self):
        return self._events

    def servers(self):
        return self._servers

    async def connect_to_peers(self):
        self.coordinator = ClientNetwork.Peer(nodeslist.coordinator, '',
                                              self.request_handler)
        result = await self.coordinator.connect()
        if result is False:
            UI.log('!!! COORDINATOR NOT FOUND !!!', level=logging.CRITICAL)

        for host, name in nodeslist.servers:
            server = ClientNetwork.Peer(host, name, self.request_handler)
            result = await server.connect()

            if result is True:
                self._servers[name] = server

    def request_handler(self, msg):
        cls = msg.__class__.__name__
        handler = getattr(self, f'handle_{cls}', None)

        if handler is None:
            UI.log(f'Dont recognize msg {cls}', level=logging.WARNING)
        else:
            UI.log(f'Got {cls}')
            handler(msg)


    class Peer:
        def __init__(self, host, name, req_handler):
            self.evloop = asyncio.get_event_loop()
            self.host = host
            self.name = name
            self.request_handler = req_handler
            self.transport = None
            self.protocol = None

        async def connect(self):
            UI.log(f'Trying connect to {self.host}')
            nodeip = socket.gethostbyname(self.host)
            try:
                transport, proto = await self.evloop.create_connection(
                    lambda: ClientNetwork.ClientProtocol(self.request_handler),
                    host=nodeip, port=ClientNetwork.PORT, family=socket.AF_INET
                )
            except ConnectionRefusedError:
                UI.log(f'Could not connect to {self.host}',
                       level=logging.WARNING)
                return False
            else:
                self.transport = transport
                self.protocol = proto
                return True

        def send(self, msg):
            msg.origin = ClientNetwork.SELF_ADDR
            msg.destination = self.protocol.peer

            UI.log(f'Sending {str(msg)}')
            pickled = pickle.dumps(msg, pickle.HIGHEST_PROTOCOL)
            self.transport.write(pickled)


    class ClientProtocol(asyncio.Protocol):
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


class Client:
    def __init__(self):
        self.ui = UI()
        self.network = ClientNetwork()

        self.ui.output('===========================================')
        self.ui.output('==== Distributed Transactions - Client ====')
        self.ui.output('===========================================')

        self.curr_txn = -1

    async def loop(self):
        await self.network.connect_to_peers()

        try:
            while True:
                command = await self.ui.input()
                if command == '':
                    continue

                cmd = command.split()[0]
                data = command.split()[1:]

                cmd_handler = getattr(self, f'cmd_{cmd.lower()}', None)
                if cmd_handler is None:
                    self.ui.output(f'Unknown command "{cmd}"...')
                else:
                    await cmd_handler(data)

        except asyncio.CancelledError:
            # self.network.close()
            self.ui.output('')
            pass

    # TODO: change 'dest' to appropriate destination (from data[1])

    async def cmd_begin(self, data):
        """
        Call coordinator, receive message id
        """
        if len(data) != 0:
            self.ui.output(f'Invalid! Usage: BEGIN')
            return

        txn_id_msg = RequestTxnID()
        event = asyncio.Event()
        self.network.events()[txn_id_msg.uid] = event, None

        self.network.coordinator.send(txn_id_msg)
        try:
            await asyncio.wait_for(event.wait(), 3)
        except asyncio.TimeoutError:
            UI.log('Failed to send RequestTxnID!', level=logging.ERROR)
            return

        response = self.network.events()[txn_id_msg.uid][1]
        self.curr_txn = response.txn_id

        self.ui.output('OK')

    async def cmd_set(self, data):
    # call server, deliver value
        msg = SetMsg()
        try:
            await asyncio.wait_for(dest.send(msg), 2, loop=self.evloop)
            await asyncio.wait_for(event.wait(), 3, loop=self.evloop)
        except asyncio.TimeoutError:
            logging.error('Failed to send setMsg!')
        # wait for response message
        # print "OK" to screen
        UI.output("OK")

    async def cmd_get(self, data):
    # call server, receive value, display to screen
        msg = GetMsg()
        try:
            await asyncio.wait_for(dest.send(msg), 2, loop=self.evloop)
            await asyncio.wait_for(event.wait(), 3, loop=self.evloop)
        except asyncio.TimeoutError:
            logging.error('Failed to send getMsg!')
        # get value from response message
        # print output to screen
        UI.output(str(data[1]) + " = " + str(value))

    async def cmd_commit(self, data):
    # call server, deliver commit or abort message
        msg = CommitMsg()
        try:
            await asyncio.wait_for(dest.send(msg), 2, loop=self.evloop)
            await asyncio.wait_for(event.wait(), 3, loop=self.evloop)
        except asyncio.TimeoutError:
            logging.error('Failed to send commitMsg!')

    async def cmd_abort(self, data):
    # call server, deliver abort
        msg = AbortMsg()
        try:
            await asyncio.wait_for(dest.send(msg), 2, loop=self.evloop)
            await asyncio.wait_for(event.wait(), 3, loop=self.evloop)
        except asyncio.TimeoutError:
            logging.error('Failed to send abortMsg!')


def main():
    logging.basicConfig(filename='client.log', level=logging.DEBUG)
    UI.log('===========================================')
    UI.log('==== Distributed Transactions - Client ====')
    UI.log('===========================================')

    evloop = asyncio.get_event_loop()
    evloop.set_debug(True)
    client = Client()

    main_task = evloop.create_task(client.loop())

    pending = None
    try:
        evloop.run_forever()
    except KeyboardInterrupt:
        pending = asyncio.Task.all_tasks(loop=evloop)
        for task in pending:
            task.cancel()

    try:
        evloop.run_until_complete(asyncio.gather(*pending))
    except asyncio.CancelledError:
        pass
    finally:
        evloop.close()

if __name__ == '__main__':
    main()
