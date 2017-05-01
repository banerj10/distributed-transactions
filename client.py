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

    async def cmd_begin(self, data):
        pass

    async def cmd_set(self, data):
        pass

    async def cmd_get(self, data):
        pass

    async def cmd_commit(self, data):
        pass

    async def cmd_abort(self, data):
        pass


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
