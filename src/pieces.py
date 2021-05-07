import logging
import math
from hashlib import sha1
from typing import List

from .torrent import BLOCK_SIZE, Torrent


class IncorrectHashException(Exception):
    pass


class Block:
    def __init__(self, piece_index: int, offset: int, length: int) -> None:
        self.index = piece_index
        self.offset = offset
        self.length = length

        self.data = None

    def __str__(self) -> str:
        return f"Block p_index={self.index} offset={self.offset}"


class Piece:
    def __init__(self, index: int, blocks: list, hash_: bytes) -> None:
        self.index = index
        self.blocks = blocks
        self.hash = hash_

        self.left = len(blocks)
        self.size = sum([block.length for block in self.blocks])

        self.connections_num = 0
        self.received = True

    def __str__(self) -> str:
        return f"Piece index={self.index} hash={self.hash}"  # type: ignore

    def blocks_generator(self):
        for block in self.blocks:
            yield block

    @property
    def data(self) -> bytes:
        return b"".join([block.data for block in self.blocks])

    def is_completed(self) -> bool:
        return self.left == 0

    def is_locked(self) -> bool:
        return self.connections_num > 0

    def unlock(self) -> None:
        self.connections_num -= 1

    def lock(self) -> None:
        self.connections_num += 1

    def is_hash_correct(self) -> bool:
        piece_hash = sha1(self.data).digest()
        return piece_hash == self.hash

    def receive(self, block_index: int, data: bytes) -> bool:
        if not self.is_completed():
            block = self.blocks[block_index]
            if block.data is None:
                self.blocks[block_index].data = data
                self.left -= 1
                return True
            return False
        return False

    def reset(self) -> None:
        self.left = len(self.blocks)
        for block in self.blocks:
            block.data = None


def initialize_pieces(torrent: Torrent) -> List[Piece]:
    pieces = []
    for i, hash_ in enumerate(torrent.pieces):
        if i != (torrent.number_of_pieces - 1):
            blocks = [
                Block(i, offset * BLOCK_SIZE, BLOCK_SIZE)
                for offset in range(torrent.blocks_in_piece)
            ]

        else:
            last_piece_length = torrent.total_size % torrent.piece_length
            number_of_blocks = math.ceil(last_piece_length / BLOCK_SIZE)
            blocks = [
                Block(i, offset * BLOCK_SIZE, BLOCK_SIZE)
                for offset in range(number_of_blocks)
            ]

            if last_piece_length % BLOCK_SIZE > 0:
                blocks[-1].length = last_piece_length % BLOCK_SIZE
        pieces.append(Piece(i, blocks, hash_))
    return pieces
