from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ElectionRequest(_message.Message):
    __slots__ = ("candidateId",)
    CANDIDATEID_FIELD_NUMBER: _ClassVar[int]
    candidateId: int
    def __init__(self, candidateId: _Optional[int] = ...) -> None: ...

class ElectionResponse(_message.Message):
    __slots__ = ("alive",)
    ALIVE_FIELD_NUMBER: _ClassVar[int]
    alive: bool
    def __init__(self, alive: bool = ...) -> None: ...

class VictoryRequest(_message.Message):
    __slots__ = ("leaderId",)
    LEADERID_FIELD_NUMBER: _ClassVar[int]
    leaderId: int
    def __init__(self, leaderId: _Optional[int] = ...) -> None: ...

class VictoryResponse(_message.Message):
    __slots__ = ("acknowledged",)
    ACKNOWLEDGED_FIELD_NUMBER: _ClassVar[int]
    acknowledged: bool
    def __init__(self, acknowledged: bool = ...) -> None: ...
