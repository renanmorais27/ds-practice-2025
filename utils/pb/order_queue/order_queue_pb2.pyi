from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BookItem(_message.Message):
    __slots__ = ("title", "quantity")
    TITLE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    title: str
    quantity: int
    def __init__(self, title: _Optional[str] = ..., quantity: _Optional[int] = ...) -> None: ...

class EnqueueRequest(_message.Message):
    __slots__ = ("orderId", "items")
    ORDERID_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    orderId: str
    items: _containers.RepeatedCompositeFieldContainer[BookItem]
    def __init__(self, orderId: _Optional[str] = ..., items: _Optional[_Iterable[_Union[BookItem, _Mapping]]] = ...) -> None: ...

class EnqueueResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class DequeueRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DequeueResponse(_message.Message):
    __slots__ = ("orderId", "found", "items")
    ORDERID_FIELD_NUMBER: _ClassVar[int]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    orderId: str
    found: bool
    items: _containers.RepeatedCompositeFieldContainer[BookItem]
    def __init__(self, orderId: _Optional[str] = ..., found: bool = ..., items: _Optional[_Iterable[_Union[BookItem, _Mapping]]] = ...) -> None: ...
