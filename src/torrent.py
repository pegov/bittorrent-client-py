import logging
import math
import os
from hashlib import sha1

from . import bencoding

BLOCK_SIZE = 2 ** 14  # 16KiB
PUBLIC_TRACKERS = [
    "udp://tracker.openbittorrent.com:6969",
]


class TorrentFile:
    def __init__(self, filename: str, offset: int, size: int) -> None:
        self.filename = filename
        self.io = open(self.filename, "wb")
        self.io.truncate(size)

        self.size = size
        self.offset = offset
        self.end = offset + size

        self.left = size
        self._completed = False

    def save(self, offset: int, data: bytes) -> None:
        self.left -= len(data)
        self.io.seek(offset)
        self.io.write(data)
        if self.left == 0:
            self._completed = True
            self.close()

    def close(self) -> None:
        if self.io and not self.io.closed:
            self.io.close()

    def is_completed(self) -> bool:
        return self._completed

    def __str__(self) -> str:
        return f"TorrentFile: filename={self.filename}, size={self.size}, offset={self.offset}"


class Torrent:
    """
    Обёртка над .torrent файлом
    """

    def __init__(self, filename: str) -> None:
        with open(filename, "rb") as f:
            content = f.read()

        self.data = bencoding.decode(content)

        self.announce_list = set()
        for sublist in self.data.get(b"announce-list"):
            for item in sublist:
                self.announce_list.add(item.decode("utf-8"))
        self.announce_list.update(PUBLIC_TRACKERS)

        self.info = self.data.get(b"info")

        pieces = self.info.get(b"pieces")
        self.pieces = [
            pieces[offset : offset + 20] for offset in range(0, len(pieces), 20)
        ]

        self.info_hash = sha1(bencoding.encode(self.info)).digest()

        files = self.info.get(b"files")
        if files:
            self.files = []
            self.dirname = self.info.get(b"name").decode("utf-8")
            path = f"./downloads/{self.dirname}"
            if not os.path.exists(path):
                os.mkdir(path)
            self.is_multi = True
            self.total_size = 0
            for file in files:
                length = file.get(b"length")
                self.files.append(
                    TorrentFile(
                        f"{path}/{file.get(b'path')[0].decode('utf-8')}",
                        self.total_size,
                        length,
                    )
                )
                self.total_size += length
        else:
            self.is_multi = False
            path = f"./downloads/{self.info.get(b'name').decode('utf-8')}"
            self.total_size = self.info.get(b"length")
            self.file = TorrentFile(path, 0, self.total_size)

        # байтов в одном куске
        self.piece_length = self.info.get(b"piece length")
        self.blocks_in_piece = self.piece_length // BLOCK_SIZE

        # кол-во кусков, включая последний
        self.number_of_pieces = math.ceil(self.total_size / self.piece_length)
        # self.number_of_pieces = len(self.pieces)
        self.number_of_blocks = (self.piece_length // BLOCK_SIZE) + int(
            bool(self.piece_length % BLOCK_SIZE)
        )

        logging.debug("------------------------")
        logging.debug("Reading torrent file...")
        logging.debug(f"Announce list: {self.announce_list}")
        logging.debug(f"is_multi: {self.is_multi}")
        if self.is_multi:
            logging.debug(f"files: {self.files}")
        else:
            logging.debug(f"file: {self.file}")
        logging.debug(f"total_size: {(self.total_size / 1000_000):.2f} mb")
        logging.debug(f"piece_length: {self.piece_length}")
        logging.debug(f"number_of_pieces: {self.number_of_pieces}")
        logging.debug(f"number_of_blocks: {self.number_of_blocks}")
        logging.debug("------------------------")
