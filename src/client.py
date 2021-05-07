import asyncio
import logging
import random
import time
from typing import Dict, List, Tuple

from .peers import Peer
from .pieces import IncorrectHashException, Piece, initialize_pieces
from .torrent import BLOCK_SIZE, Torrent
from .trackers import TrackerException, announce
from .workers import Worker


class Client:
    def __init__(self, torrent: Torrent, max_connections: int = 10) -> None:
        self.torrent = torrent
        self.max_connections = max_connections

        self.peer_id = (
            "-TE0001-" + "".join(str(random.randint(0, 9)) for _ in range(12))
        ).encode("utf-8")

        self.uploaded = 0
        self.downloaded = 0
        self.left = torrent.total_size

        self.peers: Dict[Tuple[str, int], Peer] = {}
        self.pieces = initialize_pieces(torrent)

        self.workers_queue: asyncio.Queue = asyncio.Queue()

    async def start(self) -> None:
        announce_tasks = []
        for url in self.torrent.announce_list:
            announce_tasks.append(asyncio.create_task(self._announce(url)))
        await asyncio.gather(*announce_tasks)

        logging.debug(f"Total peers: {len(self.peers)}")

        workers = []

        loop = asyncio.get_running_loop()
        self.cancel_fut = loop.create_future()
        self.finish_fut = loop.create_future()

        logging.debug("Creating workers...")
        for i in range(self.max_connections):
            worker = Worker(
                i,
                self.torrent.info_hash,
                self.peer_id,
                self.peers,
                self.workers_queue,
                self.cancel_fut,
                self._receive,
            )
            workers.append(worker)
            asyncio.create_task(worker.start())

        asyncio.create_task(self._show_status(self.left, time.time()))

        while True:
            task = asyncio.create_task(self.workers_queue.get())
            done, _ = await asyncio.wait(
                [task, self.finish_fut], return_when=asyncio.FIRST_COMPLETED
            )
            if self.finish_fut in done:
                break
            worker: Worker = task.result()  # type: ignore
            piece = self._decide_next_piece(worker.peer.pieces)
            # TODO: implement last pieces download strategy
            if piece is None:
                worker.stop()
                workers.remove(worker)
            else:
                worker.download(piece)

        logging.debug("Stopping workers...")
        tasks = []
        for worker in workers:
            worker.stop()
            tasks.append(asyncio.create_task(worker.wait_stopped()))
        await asyncio.gather(*tasks)

        logging.info("Download complete!")

    async def _announce(self, url: str, interval: int = 0) -> None:
        if interval:
            await asyncio.sleep(interval)

        try:
            res = await announce(
                url,
                self.torrent.info_hash,
                self.peer_id,
                6889,
                self.uploaded,
                self.downloaded,
                self.left,
            )
        except TrackerException:
            pass
        except NotImplementedError:
            logging.debug(f"{url}: only http and udp protocols are implemented")
        except Exception as e:
            logging.exception(e, stack_info=True)
        else:
            logging.debug(f"{url} interval {res[0]}")
            self._add_peers(res[1])
            asyncio.create_task(self._announce(url, res[0]))

    def _add_peers(self, new_peers: List[Tuple[str, int]]) -> None:
        for peer in new_peers:
            existing_peer = self.peers.get(peer)
            if existing_peer is None:
                self.peers.update({peer: Peer(peer, self.torrent.number_of_pieces)})

    def _update_downloaded(self, downloaded: int) -> None:
        self.downloaded += downloaded
        self.left -= downloaded

    def _receive(self, piece_index: int, offset: int, block: bytes) -> None:
        piece = self.pieces[piece_index]
        logging.debug(
            f"receive block: piece={piece_index}/{self.torrent.number_of_pieces - 1}  block={offset}/{piece.size}"
        )
        block_index = offset // BLOCK_SIZE
        received = piece.receive(block_index, block)
        if piece.is_completed() and received:
            if piece.is_hash_correct():
                self._save_piece(
                    piece.index * self.torrent.piece_length,
                    piece.data,
                )
                self._update_downloaded(piece.size)
                del piece.blocks
            else:
                piece.reset()
                raise IncorrectHashException()

        if self.left == 0:
            self.finish_fut.set_result(True)

    def _save_piece(self, offset: int, data: bytes) -> None:
        if self.torrent.is_multi:
            for file in self.torrent.files:
                if (
                    file.is_completed()
                    or file.offset >= offset + len(data)
                    or file.end <= offset
                ):
                    continue

                px = offset  # global piece offset start
                py = offset + len(data)  # global piece offset end
                fx = file.offset  # global file offset start
                fy = file.end  # global file offset end
                # start - local file offset
                deltax = px - fx
                deltay = py - fy
                if px <= fx:
                    start = 0
                    if py <= fy:
                        save_data = data[deltax:]
                    else:
                        save_data = data[deltax:deltay]
                else:
                    start = deltax
                    if py <= fy:
                        save_data = data[:]
                    else:
                        save_data = data[:deltay]

                file.save(start, save_data)
        else:
            self.torrent.file.save(offset, data)

    async def _show_status(self, last_left, prev) -> None:
        DELTA = 10
        await asyncio.sleep(DELTA)
        if prev:
            now = time.time()
            delta = now - prev
        delta_left = last_left - self.left
        logging.info("----------------------")
        logging.info("Status")
        logging.info(f"Downloaded: {self.downloaded} bytes")
        logging.info(f"Left: {self.left} bytes")
        logging.info(f"Average speed: {delta_left // delta} bytes / second")
        logging.info("----------------------")
        asyncio.create_task(self._show_status(self.left, now))

    def _decide_next_piece(self, peer_pieces: List[bool]) -> Piece:
        for piece, has_piece in zip(self.pieces, peer_pieces):
            if not piece.is_completed() and not piece.is_locked() and has_piece:
                return piece
        return None

        # TODO: download last piece
