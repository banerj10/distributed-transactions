import uuid

class BaseMsg:
    def __init__(self, origin='', destination=''):
        self.uid = uuid.uuid4()
        self.origin = origin
        self.destination = destination

    def type(self):
        return self.__class__.__name__

    def __str__(self):
        return str(vars(self))


class RequestTxnID(BaseMsg):
    pass


class NewTxnID(BaseMsg):
    def __init__(self, txn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.txn_id = txn_id
