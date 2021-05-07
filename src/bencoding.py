"""
Типы

Строки - <длина строки>:<строка>
4:spam, 1:a

Целые числа - i<число>e
i3e, i0e, i-3e
i03e, i-0e - нельзя

Списки - l<bencoded элементы>e
l4:spam1:ae - ["spam", "a"]
le - []

Словари - d<bencoded строка><bencoded элемент>e
d4:spam1:ae - {"spam": "a"}
de - {}
"""

import io


class DecodingError(Exception):
    pass


class EncodingError(Exception):
    pass


def _is_int(b: bytes) -> bool:
    return b"0" <= b and b <= b"9"


def _decode_int(buf) -> int:
    c = buf.read(1)
    if c == b"-":
        negative = True
        c = buf.read(1)
    else:
        negative = False

    acc = io.BytesIO()
    while c != b"e":
        if len(c) == 0:
            raise DecodingError
        if not _is_int(c):
            raise DecodingError

        acc.write(c)
        c = buf.read(1)

    v = acc.getvalue()
    if v.startswith(b"0") and len(v) > 1:
        raise DecodingError  # i0Xe - неправильно
    n = int(v)
    if n == 0 and negative:
        raise DecodingError  # i-0e - неправильно
    if negative:
        n = -n
    return n


def _decode_length(c: bytes, buf: io.BytesIO) -> int:
    acc = io.BytesIO()
    while c != b":":
        if not _is_int(c):
            raise DecodingError
        acc.write(c)
        c = buf.read(1)
    return int(acc.getvalue())


def _decode_string(c: bytes, buf: io.BytesIO) -> bytes:
    length = _decode_length(c, buf)
    s = buf.read(length)
    if len(s) != length:
        raise DecodingError
    return s


def _list_to_dict(it) -> dict:
    it = it[::-1]
    zipped = [
        (
            it[i - 1],
            it[i],
        )
        for i in range(1, len(it), 2)
    ]
    # ключ не может быть не строкой
    if not all(isinstance(k, bytes) for k, _ in zipped):
        raise DecodingError
    return dict(zipped)


list_t = object()
dict_t = object()


def decode(s: bytes) -> dict:
    buf = io.BytesIO(s)
    stack = []  # type: ignore

    while True:
        c = buf.read(1)
        if not c:
            raise DecodingError

        if c == b"e":
            acc = []  # type: ignore
            while True:
                if not stack:
                    raise DecodingError
                x = stack.pop()
                if x == list_t:
                    elem = list(reversed(acc))
                    break
                elif x == dict_t:
                    elem = _list_to_dict(acc)  # type: ignore
                    break
                else:
                    acc.append(x)

        elif c == b"i":
            elem = _decode_int(buf)  # type: ignore
        elif c == b"d":
            stack.append(dict_t)
            continue
        elif c == b"l":
            stack.append(list_t)
            continue
        else:
            elem = _decode_string(c, buf)  # type: ignore

        if not stack:
            end = buf.read(1)
            if end:
                raise DecodingError
            return elem  # type: ignore
        else:
            stack.append(elem)


def encode(obj) -> bytes:
    if type(obj) == str:
        return _encode_string(obj)
    elif type(obj) == bytes:
        return _encode_bytes(obj)
    elif type(obj) == int:
        return _encode_int(obj)
    elif type(obj) == list:
        return _encode_list(obj)
    elif type(obj) == dict:
        return _encode_dict(obj)
    else:
        raise EncodingError


def _encode_string(value: str) -> bytes:
    res = str(len(value)) + ":" + value
    return res.encode("ascii")


def _encode_int(value: int) -> bytes:
    return b"i" + str(value).encode("ascii") + b"e"


def _encode_bytes(value: bytes) -> bytes:
    return str(len(value)).encode("ascii") + b":" + value


def _encode_list(value: list) -> bytes:
    return b"l" + b"".join([encode(item) for item in value]) + b"e"


def _encode_dict(d: dict) -> bytes:
    res = b"d"
    for k in sorted(d.keys()):  # ключи должны быть отсортированы как строки
        if type(k) not in (
            str,
            bytes,
        ):
            raise EncodingError
        key = encode(k)
        value = encode(d[k])
        res += key
        res += value
    res += b"e"
    return res
