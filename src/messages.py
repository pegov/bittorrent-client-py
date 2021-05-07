import struct
from typing import List

from bitarray import bitarray


class Handshake:
    """
    offset size name         value
    0      u8   name length  19
    1      19b  protocol     BitTorrent protocol
    20     8b   reserved
    28     20b  info_hash
    48     20b  peer-id
    68
    """

    name_length = 19
    protocol = b"BitTorrent protocol"

    def __init__(self, info_hash: bytes, peer_id: bytes) -> None:
        self.info_hash = info_hash
        self.peer_id = peer_id

    @classmethod
    def encode(cls, info_hash: bytes, peer_id: bytes) -> bytes:
        return struct.pack(
            ">b19s8x20s20s", cls.name_length, cls.protocol, info_hash, peer_id
        )

    @classmethod
    def decode(cls, data: bytes) -> "Handshake":
        payload = struct.unpack(">B19s8x20s20s", data)
        return cls(payload[2], payload[3])


"""
<len=XXXX><id=X><payload>
"""


class KeepAlive:
    """
    len = 0
    2 minutes
    """

    len = 0

    @classmethod
    def encode(cls) -> bytes:
        return struct.pack(">L", cls.len)


class Choke:
    """
    len = 1
    id  = 0
    """

    len = 1
    id = 0

    @classmethod
    def encode(cls) -> bytes:
        return struct.pack(">LB", cls.len, cls.id)


class Unchoke:
    """
    len = 1
    id  = 1
    """

    len = 1
    id = 1

    @classmethod
    def encode(cls) -> bytes:
        return struct.pack(">LB", cls.len, cls.id)


class Interested:
    """
    len = 1
    id  = 2
    """

    len = 1
    id = 2

    @classmethod
    def encode(cls) -> bytes:
        return struct.pack(">LB", cls.len, cls.id)


class NotInterested:
    """
    len = 1
    id  = 3
    """

    len = 1
    id = 3

    @classmethod
    def encode(cls) -> bytes:
        return struct.pack(">LB", cls.len, cls.id)


class Have:
    """
    len = 5
    id  = 4
    piece_index
    """

    len = 5
    id = 4

    def __init__(self, piece_index: int) -> None:
        self.piece_index = piece_index

    @classmethod
    def encode(cls, piece_index) -> bytes:
        return struct.pack(">LBL", cls.len, cls.id, piece_index)

    @classmethod
    def decode(cls, data: bytes) -> "Have":
        piece_index = struct.unpack(">L", data)[0]
        return cls(piece_index)


class Bitfield:
    """
    len = 1 + X
    id  = 5
    bitfield
    """

    len = 1
    id = 5

    def __init__(self, bitfield: List[int]) -> None:
        self.bitfield = bitfield

    @classmethod
    def encode(cls, bitfield: List[int]) -> bytes:
        payload = bitarray()
        payload.extend(bitfield)
        return struct.pack(
            f">LB{len(bitfield) / 8 }s",
            cls.len + len(bitfield) / 8,
            cls.id,
            payload.tobytes(),
        )

    @classmethod
    def decode(cls, data: bytes) -> "Bitfield":
        a = bitarray()
        a.frombytes(data)
        return cls(a.tolist())


class Request:
    """
    len = 13
    id  = 6
    index
    begin
    length
    """

    len = 13
    id = 6

    def __init__(self, index: int, begin: int, length: int) -> None:
        self.index = index
        self.begin = begin
        self.length = length

    @classmethod
    def encode(cls, index: int, begin: int, length: int) -> bytes:
        return struct.pack(">LBLLL", cls.len, cls.id, index, begin, length)

    @classmethod
    def decode(cls, data: bytes) -> "Request":
        index, begin, length = struct.unpack(">LLL", data)
        return cls(index, begin, length)


class Piece:
    """
    len = 9 + X
    id  = 7
    index
    begin
    block
    """

    len = 9
    id = 7

    def __init__(self, piece_index: int, begin: int, block: bytes) -> None:
        self.index = piece_index
        self.begin = begin
        self.block = block

    @classmethod
    def encode(cls, piece_index: int, begin: int, block: bytes) -> bytes:
        return struct.pack(
            f">LBLL{len(block)}s",
            cls.len + len(block),
            cls.id,
            piece_index,
            begin,
            block,
        )

    @classmethod
    def decode(cls, data: bytes) -> "Piece":
        piece_index, begin, block = struct.unpack(f">LL{len(data) - 8}s", data)
        return cls(piece_index, begin, block)


class Cancel:
    """
    len = 13
    id  = 8
    """

    len = 13
    id = 8

    def __init__(self, index: int, begin: int, length: int) -> None:
        self.index = index
        self.begin = begin
        self.lenth = length

    @classmethod
    def encode(cls, index: int, begin: int, length: int) -> bytes:
        return struct.pack(">LBLLL", cls.len, cls.id, index, begin, length)

    @classmethod
    def decode(cls, data: bytes) -> "Cancel":
        index, begin, length = struct.unpack(">LLL", data)
        return cls(index, begin, length)
