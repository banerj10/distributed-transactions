import asyncio
import logging
import sys


class UI:
    def __init__(self):
        self.should_return = False
        self.outfile = None
        self.queue = asyncio.Queue()
        asyncio.get_event_loop().add_reader(sys.stdin, self._on_input)

    def _on_input(self):
        asyncio.ensure_future(self.queue.put(sys.stdin.readline()))

    async def input(self, prompt=None):
        if prompt is None:
            prompt = '>>> '
        print(prompt, end='', flush=True)
        inp = await self.queue.get()
        return inp.strip()

    def output(self, msg):
        if self.should_return:
            self.outfile.write(str(msg))
            self.outfile.write('\n')
        else:
            print(str(msg))

    @staticmethod
    def log(msg, level=logging.INFO):
        logging.log(level, msg)

    def set_output_to_return(self, should_return, outfile=None):
        self.should_return = should_return
        self.outfile = outfile
