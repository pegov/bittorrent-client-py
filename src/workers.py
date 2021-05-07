import asyncio
import logging
import random
import struct
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import messages
from .peers import Peer
from .pieces import Block, IncorrectHashException, Piece

CHUNK_SIZE = 2 ** 14 + 9
HANDSHAKE_LENGTH = 68
KEEP_ALIVE_INTERVAL = 120
INTERESTED_TIMEOUT = 120


class HandshakeException(Exception):
    pass


class InterestedTimeoutException(Exception):
    pass


class ShutdownException(Exception):
    pass


class Worker:
    def __init__(
        self,
        i: int,
        info_hash: bytes,
        peer_id: bytes,
        peers: Dict[Tuple[str, int], Peer],
        workers_queue: asyncio.Queue,
        cancel_fut: asyncio.Future,
        on_recv: Callable,
    ) -> None:
        self.i = i
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.peers = peers
        self.workers_queue = workers_queue
        self.cancel_fut = cancel_fut
        self.on_recv = on_recv

        # status
        self.am_choking = True
        self.am_interested = False
        self.peer_choking = True
        self.peer_interested = False

        self.ready = False
        self.waiting = False
        self.waiting_blocks: List[Block] = []

        loop = asyncio.get_running_loop()
        self.piece_fut = loop.create_future()
        self.piece: Optional[Piece] = None
        self.peer: Optional[Peer] = None

        self.stop_fut = loop.create_future()
        self.stopped_fut = loop.create_future()

        self.interested_task: Optional[asyncio.Task] = None
        self.interested_fut = loop.create_future()

        self.keep_alive_task: Optional[asyncio.Task] = None

    def download(self, piece: Piece) -> None:
        self.piece_fut.set_result(piece)

    def reset_fut(self) -> None:
        loop = asyncio.get_running_loop()
        self.piece_fut = loop.create_future()

    async def start(self) -> None:
        connect_task = asyncio.create_task(self._connect())
        done, _ = await asyncio.wait(
            [connect_task, self.cancel_fut, self.stop_fut],
            return_when=asyncio.FIRST_COMPLETED,
        )
        if connect_task not in done:
            connect_task.cancel()

    def stop(self) -> None:
        self.stop_fut.set_result(True)

    async def wait_stopped(self) -> None:
        await self.stopped_fut

    async def _connect(self) -> None:
        reader, writer = None, None
        block: Optional[Block] = None
        data = b""
        try:
            while True:
                data = b""
                tries = 0
                while True:
                    self.peer = _get_peer(self.peers)
                    if self.peer is not None:
                        break
                    else:
                        await asyncio.sleep(15)
                        tries += 1
                        if tries > 10:
                            raise ShutdownException()
                self.peer.lock()

                try:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(self.peer.ip, self.peer.port), 10
                    )
                except (
                    ConnectionError,
                    TimeoutError,
                    OSError,
                    asyncio.TimeoutError,
                ) as e:
                    logging.warning(
                        f"Worker #{self.i}: can't connect to peer {self.peer.ip}:{self.peer.port} - {type(e)}"
                    )
                    self.peer.invalidate()
                    continue
                except Exception as e:
                    logging.exception(e, stack_info=True)
                    raise e

                try:
                    logging.debug(f"Worker #{self.i}: sending handshake")
                    await _send_handshake(writer, self.info_hash, self.peer_id)
                    data, peer_id = await asyncio.wait_for(
                        _receive_handshake(reader, self.info_hash, self.peer), 15
                    )
                    logging.debug(f"Worker #{self.i}: received handshake")
                    self.peer.set_peer_id(peer_id)
                except (
                    ConnectionError,
                    TimeoutError,
                    OSError,
                    asyncio.TimeoutError,
                    HandshakeException,
                ):
                    logging.warning(
                        f"Worker #{self.i}: can't receive handshake from peer {self.peer.ip}:{self.peer.port}"
                    )
                    self.peer.invalidate()
                    continue
                except Exception as e:
                    logging.exception(e, stack_info=True)
                    raise e

                self._reset_keep_alive_task(writer)

                try:
                    read_task: Optional[asyncio.Task] = None
                    while True:
                        if self.piece and self.piece.is_completed():
                            logging.debug(
                                f"Worker #{self.i}: piece and piece_completed"
                            )
                            self.ready = False
                            self.waiting = False
                            self.piece.unlock()
                            self.piece = None

                        if self.peer.has_bitfield() and not self.ready:
                            logging.debug(
                                f"Worker #{self.i}: has_bitfield and not ready"
                            )
                            self.workers_queue.put_nowait(self)
                            self.ready = True

                        if (
                            self.piece
                            and not self.piece.is_completed()
                            and not self.am_interested
                            and self.peer_choking
                            and not self.waiting
                        ):
                            logging.debug(f"Worker #{self.i}: sending interested")
                            self.am_interested = True
                            await _send_interested(writer)
                            self._reset_keep_alive_task(writer)
                            self.interested_task = asyncio.create_task(
                                self._interested_task_run()
                            )

                        if self.piece and not self.peer_choking and not self.waiting:
                            logging.debug(f"Worker #{self.i}: sending requests")
                            for block in self.piece.blocks:
                                if block.data is None:
                                    await _send_request(writer, block)
                            self._reset_keep_alive_task(writer)
                            self.waiting = True

                        if read_task is None:
                            if self.peer.has_bitfield():
                                wait_time = 60
                            else:
                                wait_time = 30
                            read_task = asyncio.create_task(
                                asyncio.wait_for(reader.read(CHUNK_SIZE), wait_time)
                            )
                        done, _ = await asyncio.wait(
                            [read_task, self.piece_fut, self.interested_fut],
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if self.interested_fut in done:
                            raise InterestedTimeoutException()

                        if self.piece_fut in done:
                            self.piece = self.piece_fut.result()
                            self.piece.lock()
                            logging.debug(
                                f"Worker #{self.i}: piece to download - {self.piece}"
                            )
                            self._reset_piece_fut()

                        if read_task in done:
                            res = read_task.result()
                            if not res:  # EOF
                                break
                            data += res
                            data, message = _parse_data(data)
                            if message is not None:
                                logging.debug(f"Worker #{self.i}: {message}")
                                self._resolve_message(message, self.peer)
                                if type(message) is messages.Unchoke:
                                    self._cancel_interested_task()
                            read_task = None

                        if self.peer_choking and self.waiting:
                            logging.debug(f"Worker #{self.i}: peer_choking and waiting")
                            self.am_interested = False
                            self.waiting = False

                except (
                    ConnectionError,
                    OSError,
                    TimeoutError,
                    asyncio.TimeoutError,
                ) as e:
                    logging.debug(
                        f"Worker #{self.i}: close after handshake {self.peer.ip}:{self.peer.port} - {type(e)}"
                    )
                    # if not self.peer.has_bitfield():
                    # self.peer.invalidate()
                    continue
                except InterestedTimeoutException:
                    self._reset_interested_fut()
                    self.peer.invalidate()
                    logging.debug(
                        f"Worker #{self.i}: interested timeout {self.peer.ip}:{self.peer.port}"
                    )
                    continue
                except IncorrectHashException:
                    self.peer.invalidate()
                    logging.debug(
                        f"Worker #{self.i}: incorrect hash. Discarding {self.peer.ip}:{self.peer.port}"
                    )
                finally:
                    if self.peer:
                        self.peer.unlock()
                    if self.piece:
                        self.piece.unlock()
                    self._reset_status()
                    self._cancel_keep_alive_task()
                    await _close(writer)
        except ShutdownException:
            logging.info(f"Worker #{self.i}: no more peers")
        except asyncio.CancelledError:
            logging.info(f"Worker #{self.i}: task was cancelled")
        finally:
            await _close(writer)

        self.stopped_fut.set_result(True)

    async def _interested_task_run(self) -> None:
        try:
            await asyncio.sleep(INTERESTED_TIMEOUT)
            if not self.interested_task.cancelled():
                self.interested_fut.set_result(True)
        except asyncio.CancelledError:
            logging.debug("interested was cancelled")

    def _cancel_interested_task(self) -> None:
        self.interested_task.cancel()

    def _reset_interested_fut(self) -> None:
        loop = asyncio.get_running_loop()
        self.interested_fut = loop.create_future()

    def _cancel_keep_alive_task(self) -> None:
        if self.keep_alive_task is not None and not self.keep_alive_task.cancelled():
            self.keep_alive_task.cancel()

    def _reset_keep_alive_task(self, writer: asyncio.StreamWriter) -> None:
        self._cancel_keep_alive_task()
        self.keep_alive_task = asyncio.create_task(_send_keep_alive(writer))

    def _reset_status(self) -> None:
        self.am_choking = True
        self.am_interested = False
        self.peer_choking = True
        self.peer_interested = False
        self.ready = False
        self.waiting = False
        self.piece = None

    def _reset_piece_fut(self) -> None:
        loop = asyncio.get_running_loop()
        self.piece_fut = loop.create_future()

    def _resolve_message(self, message, peer) -> None:
        if type(message) is messages.KeepAlive:
            # Другой пир тоже кидает keepalive, когда хочет приостановить отдачу
            # TODO: reset timeouts
            pass
        elif type(message) is messages.Choke:
            self.peer_choking = True
        elif type(message) is messages.Unchoke:
            self.peer_choking = False
        elif type(message) is messages.Interested:
            self.peer_interested = True
        elif type(message) is messages.NotInterested:
            self.peer_interested = False
        elif type(message) is messages.Have:
            peer.have(message.piece_index)
        elif type(message) is messages.Bitfield:
            peer.bitfield(message.bitfield)
        elif type(message) is messages.Request:
            pass
        elif type(message) is messages.Piece:
            self.on_recv(message.index, message.begin, message.block)

        elif type(message) is messages.Cancel:
            pass  # not impl


def _parse_data(data: bytes) -> Tuple[bytes, Optional[Any]]:
    if len(data) < 4:
        return data, None

    message_length = struct.unpack(">L", data[:4])[0]
    if message_length == 0:
        return data, messages.KeepAlive

    if len(data) < message_length + 4:
        logging.debug("Not enough data to parse payload")
        return data, None

    def _consume(buf: bytes) -> bytes:
        offset = message_length + 4
        return buf[offset:]

    def _extract_and_consume(buf: bytes) -> Tuple[bytes, bytes]:
        payload = buf[5 : message_length + 4]
        buf = _consume(buf)
        return buf, payload

    message_id = struct.unpack(">B", data[4:5])[0]
    if message_id == messages.Choke.id:
        data = _consume(data)
        return data, messages.Choke()

    elif message_id == messages.Unchoke.id:
        data = _consume(data)
        return data, messages.Unchoke()

    elif message_id == messages.Interested.id:
        data = _consume(data)
        return data, messages.Interested()

    elif message_id == messages.NotInterested.id:
        data = _consume(data)
        return data, messages.NotInterested()

    elif message_id == messages.Have.id:
        data, payload = _extract_and_consume(data)
        return data, messages.Have.decode(payload)

    elif message_id == messages.Bitfield.id:
        data, payload = _extract_and_consume(data)
        return data, messages.Bitfield.decode(payload)

    elif message_id == messages.Request.id:
        data, payload = _extract_and_consume(data)
        return data, messages.Request.decode(payload)

    elif message_id == messages.Piece.id:
        data, payload = _extract_and_consume(data)
        return data, messages.Piece.decode(payload)

    elif message_id == messages.Cancel:
        data, payload = _extract_and_consume(data)
        return data, messages.Cancel.decode(payload)

    else:
        logging.debug(f"message_id = {message_id} is not implemented")

    return data, None


def _get_peer(peers: Dict[Tuple[str, int], Peer]) -> Optional[Peer]:
    free_peers = [
        peer for peer in peers.values() if not peer.is_locked() and peer.is_valid()
    ]
    if len(free_peers) > 0:
        return random.choice(free_peers)
    return None


async def _send_handshake(
    writer: asyncio.StreamWriter, info_hash: bytes, peer_id: bytes
) -> None:
    writer.write(messages.Handshake.encode(info_hash, peer_id))
    await writer.drain()


async def _receive_handshake(
    reader, info_hash: bytes, peer: Peer
) -> Tuple[bytes, bytes]:
    buf = b""
    tries = 1
    while len(buf) < HANDSHAKE_LENGTH and tries < 10:
        tries += 1
        buf += await reader.read(CHUNK_SIZE)

    if len(buf) < 68:
        raise HandshakeException("buf < 68")

    handshake = messages.Handshake.decode(buf[:HANDSHAKE_LENGTH])

    if not handshake.info_hash == info_hash:
        raise HandshakeException("info hashes are not equal")

    return buf[HANDSHAKE_LENGTH:], handshake.peer_id


async def _send_interested(writer: asyncio.StreamWriter) -> None:
    writer.write(messages.Interested.encode())
    await writer.drain()


async def _send_request(writer: asyncio.StreamWriter, block: Block) -> None:
    writer.write(messages.Request.encode(block.index, block.offset, block.length))
    await writer.drain()


async def _send_keep_alive(writer: asyncio.StreamWriter) -> None:
    try:
        await asyncio.sleep(KEEP_ALIVE_INTERVAL)
        writer.write(messages.KeepAlive.encode())
        await writer.drain()
    except asyncio.CancelledError:
        # отменяется после каждого writer.write()
        pass
    except Exception as e:
        # writer уже может быть закрыт
        logging.exception("Keep alive cancel error")
        logging.exception(e)


async def _close(writer: asyncio.StreamWriter) -> None:
    try:
        if writer is not None:
            writer.is_closing()
            writer.close()
            await writer.wait_closed()
    except Exception as e:
        logging.exception(f"Close writer exception: {type(e)}")
