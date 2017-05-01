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
    def __init__(self, orig_uid, txn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orig_uid = orig_uid
        self.txn_id = txn_id


class SetMsg(BaseMsg):
    def __init__(self, txn_id, key, value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.txn_id = txn_id
        self.key = key
        self.value = value


class SetMsgResponse(BaseMsg):
    def __init__(self, orig_uid, success, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orig_uid = orig_uid
        self.success = success


class GetMsg(BaseMsg):
    def __init__(self, txn_id, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.txn_id = txn_id
        self.key = key


class GetMsgResponse(BaseMsg):
    def __init__(self, orig_uid, success, value='', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orig_uid = orig_uid
        self.success = success
        self.value = value


class TryCommitMsg(BaseMsg):
    def __init__(self, txn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.txn_id = txn_id


class TryCommitMsgResponse(BaseMsg):
    def __init__(self, orig_uid, success, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orig_uid = orig_uid
        self.success = success


class DoCommitMsg(BaseMsg):
    def __init__(self, txn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.txn_id = txn_id


class AbortMsg(BaseMsg):
    def __init__(self, txn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.txn_id = txn_id


class AbortMsgResponse(BaseMsg):
    def __init__(self, orig_uid, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orig_uid = orig_uid
