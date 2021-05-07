import asyncio
import ipaddress
import logging
import random
import struct
from asyncio.futures import Future
from typing import List, Optional, Tuple
from urllib.parse import urlencode

from httpx import AsyncClient

from . import bencoding


class TrackerException(Exception):
    pass


async def announce(
    url: str,
    info_hash: bytes,
    peer_id: bytes,
    port: int,
    uploaded: int,
    downloaded: int,
    left: int,
) -> Tuple[int, List[Tuple[str, int]]]:
    logging.debug(f"announce: {url}")
    if url.startswith("udp"):
        tracker = UDPTracker()
    elif url.startswith("http"):
        tracker = HTTPTracker()  # type: ignore
    else:
        raise NotImplementedError()
    return await tracker.announce(
        url, info_hash, peer_id, port, uploaded, downloaded, left
    )


def _unpack_peers_in_binary_model(peers_raw: bytes, count: int, offset: int):
    peers = []
    for i in range(count):
        end = offset + 6 * (i + 1)
        try:
            ip, port = struct.unpack_from(
                ">LH", peers_raw[0:end], offset=(offset + (i * 6))
            )
        except struct.error:
            continue
        ip_str = str(ipaddress.ip_address(ip))
        if port != 0 and ip_str != "0.0.0.0":
            peers.append(
                (
                    ip_str,
                    port,
                )
            )
    return peers


class AbstractTracker:
    async def announce(
        self,
        url: str,
        info_hash: bytes,
        peer_id: bytes,
        port: int,
        uploaded: int,
        downloaded: int,
        left: int,
    ) -> Tuple[int, List[Tuple[str, int]]]:
        raise NotImplementedError


class HTTPTracker(AbstractTracker):
    @staticmethod
    def _encode_info_hash(info_hash: bytes) -> str:
        return urlencode({"info_hash": info_hash}).split("=", 1)[1]

    @staticmethod
    def _read_response(res: bytes) -> Tuple[int, List[Tuple[str, int]]]:
        data = bencoding.decode(res)

        failure_reason = data.get(b"failure reason")
        if failure_reason:
            raise TrackerException(failure_reason)

        interval = data.get(b"interval")
        seeders = data.get(b"complete")
        leechers = data.get(b"incomplete")
        peers = data.get(b"peers")

        if isinstance(peers, list):
            peer_addrs = [
                (
                    peer.get(b"ip"),
                    peer.get(b"port"),
                )
                for peer in peers
            ]
        else:
            peer_addrs = _unpack_peers_in_binary_model(
                peers, seeders + leechers, offset=0
            )

        return interval, peer_addrs

    async def announce(
        self,
        url: str,
        info_hash: bytes,
        peer_id: bytes,
        port: int,
        uploaded: int,
        downloaded: int,
        left: int,
    ) -> Tuple[int, List[Tuple[str, int]]]:
        async with AsyncClient() as client:
            res = await client.get(
                url,
                params={
                    "info_hash": self._encode_info_hash(info_hash),
                    "peer_id": peer_id.decode("utf-8"),
                    "port": port,
                    "uploaded": uploaded,
                    "downloaded": downloaded,
                    "left": left,
                    "compact": 1,
                    # "event": "started",
                },
            )
            if res.status_code != 200:
                raise TrackerException("http status code != 200")
            return self._read_response(res.read())


class UDPTracker(AbstractTracker):
    async def announce(
        self,
        url: str,
        info_hash: bytes,
        peer_id: bytes,
        port: int,
        uploaded: int,
        downloaded: int,
        left: int,
    ) -> Tuple[int, List[Tuple[str, int]]]:
        s = url.split("/")[2]
        addr, addr_port = s.split(":")

        loop = asyncio.get_running_loop()
        on_con_lost_fut = loop.create_future()
        result_fut = loop.create_future()
        transport = None
        try:
            transport, _ = await loop.create_datagram_endpoint(
                lambda: UDPTrackerConnection(  # type: ignore
                    on_con_lost_fut,
                    result_fut,
                    info_hash,
                    peer_id,
                    port,
                    uploaded,
                    downloaded,
                    left,
                ),
                remote_addr=(addr, int(addr_port)),
            )
            await asyncio.wait_for(on_con_lost_fut, timeout=15)
            if result_fut.done():
                return result_fut.result()
        except (TimeoutError, OSError, asyncio.TimeoutError) as e:
            logging.debug(f"{url}: {type(e)}")
            raise TrackerException("UDP Connection error")
        finally:
            if transport is not None and not transport.is_closing():
                transport.close()


class UDPTrackerConnection:
    def __init__(
        self,
        on_con_lost: Future,
        result: Future,
        info_hash: bytes,
        peer_id: bytes,
        port: int,
        uploaded: int,
        downloaded: int,
        left: int,
    ):
        self.on_con_lost = on_con_lost
        self.result = result

        self.transport: Optional[asyncio.DatagramTransport] = None

        self.info_hash = info_hash
        self.peer_id = peer_id
        self.port = port
        self.uploaded = uploaded
        self.downloaded = downloaded
        self.left = left

    def connection_made(self, transport):
        self.transport = transport
        self._create_new_transaction_id()
        self._send_connect_input()

    def datagram_received(self, data: bytes, addr):
        if len(data) == 16:
            self._verify_connect_output(data)
            self._create_new_transaction_id()
            self._send_announce_input()
        elif len(data) >= 20:
            self._verify_announce_output(data)
            self.transport.close()

    def error_received(self, e):
        pass

    def connection_lost(self, e):
        if not self.on_con_lost.done():
            self.on_con_lost.set_result(True)

    def _create_new_transaction_id(self) -> None:
        self.transaction_id = random.randint(10 << 4, 10 << 9)

    def _send_connect_input(self):
        """
        offset size name            value
        0      u64    connection_id   0x41727101980
        8      u32    action          0 (connect)
        12     u32    transaction_id  random
        16
        """
        connect_input = struct.pack(">QLL", 0x41727101980, 0, self.transaction_id)
        self.transport.sendto(connect_input)

    def _verify_connect_output(self, data: bytes) -> None:
        """
        offset size   name            value
        0      u32    action          0 (connect)
        4      u32    transaction_id
        8      u64    connection_id
        """
        action, transaction_id, connection_id = struct.unpack(">LLQ", data)
        if self.transaction_id != transaction_id or action != 0:
            self.transport.close()

        self.connection_id = connection_id

    def _send_announce_input(self) -> None:
        """
        offset size   name            value
        0      u64    connection_id
        8      u32    action          1 (announce)
        12     u32    transaction_id  random
        16     20b    info_hash
        36     20b    peer_id         -XXYYYY-ZZZZZZZZZZZZ
        56     u64    downloaded
        64     u64    left
        72     u64    uploaded
        80     u32    event           ???
        84     u32    ip addr         0
        88     u32    key             ???
        92     i32    num_want        -1
        96     u16    port
        98
        """
        message = (
            struct.pack(
                "!QLL",
                self.connection_id,
                1,
                self.transaction_id,
            )
            + self.info_hash
            + self.peer_id
            + struct.pack(
                ">QQQLLLlH",
                self.downloaded,
                self.left,
                self.uploaded,
                0,
                0,
                0,
                -1,
                self.port,
            )
        )
        self.transport.sendto(message)

    def _verify_announce_output(self, data: bytes) -> None:
        """
        offset size   name            value
        0      u32    action          1 (announce)
        4      u32    transaction_id
        8      u32    interval
        12     u32    leechers
        16     u32    seeders
        20+6*n u32    ip addrs
        24+6*n u16    TCP port
        20+6*N

        Do not announce again until interval seconds have passed or an
        event has happened
        """
        offset = 20

        action, transaction_id, interval, leechers, seeders = struct.unpack(
            ">LLLLL", data[0:offset]
        )
        if not transaction_id == self.transaction_id or action != 1:
            self.transport.close()
            return

        peers = _unpack_peers_in_binary_model(data, leechers + seeders, offset)
        self.result.set_result(
            (
                interval,
                peers,
            )
        )
