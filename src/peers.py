from typing import List, Optional, Tuple


class Peer:
    def __init__(self, peer: Tuple[str, int], total_pieces: int) -> None:
        self.ip, self.port = peer
        self.pieces = [False for _ in range(total_pieces)]

        self.peer_id: Optional[bytes] = None
        self.locked = False
        self._has_bitfield = False
        self.valid = True

    def did_handshake(self) -> bool:
        return self.peer_id is not None

    def is_valid(self) -> bool:
        return self.valid

    def invalidate(self) -> None:
        self.valid = False

    def is_locked(self) -> bool:
        return self.locked

    def lock(self) -> None:
        self.locked = True

    def unlock(self) -> None:
        self.locked = False

    def set_peer_id(self, peer_id: bytes) -> None:
        self.peer_id = peer_id

    def have(self, i: int) -> None:
        """
        have message
        """
        self.pieces[i] = True

    def bitfield(self, bitfield: List[int]) -> None:
        """
        bitfield message
        """
        self.pieces = [bool(bit) for bit in bitfield]
        self._has_bitfield = True

    def has_bitfield(self) -> bool:
        return self._has_bitfield

    def has_piece(self, i: int) -> bool:
        return self.pieces[i]

    def __str__(self) -> str:
        return f"Peer {self.ip}:{self.port}"
