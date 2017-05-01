import asyncio
import logging

from ui import UI

class Client:
    def __init__(self):
        self.ui = UI()

    async def loop(self):
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
    # call coordinator, receive message id
        msg = RequestTxnID()
        try:
            await asyncio.wait_for(dest.send(msg), 2, loop=self.evloop)
            await asyncio.wait_for(event.wait(), 3, loop=self.evloop)
        except asyncio.TimeoutError:
            logging.error('Failed to send beginMsg!')
        # get TxnID from response message
        # store TxnID in loop  

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
    logging.basicConfig(filename='app.log', level=logging.DEBUG)
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
