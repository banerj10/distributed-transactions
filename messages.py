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


class SetMsg(BaseMsg):
    def __init__(self, key, value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key
        self.value = value


class SetMsgResponse(BaseMsg):
    def __init__(self, orig_uid, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orig_uid = orig_uid


class GetMsg(BaseMsg):
    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key


class GetMsgResponse(BaseMsg):
    def __init__(self, orig_uid, value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orig_uid = orig_uid
        self.value = value


class CommitMsg(BaseMsg):
    pass


class CommitMsgResponse(BaseMsg):
    def __init__(self, orig_uid, response, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orig_uid = orig_uid
        self.response = response


class AbortMsg(BaseMsg):
    pass


class AbortMsgResponse(BaseMsg):
    def __init__(self, orig_uid, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orig_uid = orig_uid



