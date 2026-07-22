"""Hand-built MDF4 data-block bytes for unit tests (DT/DZ/DL/HL)."""

from __future__ import annotations

import io
import struct
import zlib
from typing import List, Tuple

import numpy as np


def make_dt_block(payload: bytes) -> bytes:
    """Build a ##DT block containing ``payload``."""
    length = 24 + len(payload)
    return b"##DT" + b"\x00" * 4 + struct.pack("<Q", length) + struct.pack("<Q", 0) + payload


def make_dz_block(
    payload: bytes,
    zip_type: int = 0,
    zip_parameter: int = 0,
) -> bytes:
    """Build a ##DZ block; ``zip_type=1`` stores payload column-major before zlib."""
    to_compress = payload
    if zip_type == 1:
        cols = zip_parameter
        arr = np.frombuffer(payload, dtype=np.uint8)
        rows = len(arr) // cols
        remainder = len(arr) % cols
        if remainder == 0:
            to_compress = arr.reshape(rows, cols).T.tobytes()
        else:
            col_major = bytearray()
            for col in range(cols):
                col_size = rows + (1 if col < remainder else 0)
                for row in range(col_size):
                    col_major.append(arr[row * cols + col])
            to_compress = bytes(col_major)
    compressed = zlib.compress(to_compress)
    org_size = len(payload)
    data_length = len(compressed)
    length = 48 + data_length
    header = (
        b"##DZ"
        + b"\x00" * 4
        + struct.pack("<Q", length)
        + struct.pack("<Q", 0)
        + b"\x00\x00"
        + struct.pack("<B", zip_type)
        + b"\x00"
        + struct.pack("<I", zip_parameter)
        + struct.pack("<Q", org_size)
        + struct.pack("<Q", data_length)
    )
    return header + compressed


def make_dl_block(next_dl: int, data_addrs: List[int]) -> bytes:
    """Build a ##DL block pointing at ``data_addrs`` (link 0 = next_dl)."""
    dl_count = len(data_addrs)
    link_count = 1 + dl_count
    links = struct.pack(f"<{link_count}Q", next_dl, *data_addrs)
    body = links + b"\x00" + b"\x00" * 3 + struct.pack("<I", dl_count)
    length = 24 + len(body)
    return (
        b"##DL"
        + b"\x00" * 4
        + struct.pack("<Q", length)
        + struct.pack("<Q", link_count)
        + body
    )


def make_hl_block(dl_addr: int) -> bytes:
    """Build a ##HL block whose first link points at ``dl_addr``."""
    link_count = 1
    length = 24 + 8 * link_count
    return (
        b"##HL"
        + b"\x00" * 4
        + struct.pack("<Q", length)
        + struct.pack("<Q", link_count)
        + struct.pack("<Q", dl_addr)
    )


def build_dl_file(
    dt_payloads: List[bytes],
    wrap_hl: bool = False,
) -> Tuple[bytes, int]:
    """Lay out DT sub-blocks + DL (optionally HL) in one buffer.

    Returns ``(file_bytes, data_block_addr)`` where ``data_block_addr`` is the
    address passed to ``read_raw_data`` / ``parse_subblocks`` (DL or HL).
    """
    # Avoid placing DT blocks at file offset 0 — the DL reader treats link 0 as null.
    blob = bytearray(b"\x00" * 8)
    dt_addrs: List[int] = []
    for payload in dt_payloads:
        dt_addrs.append(len(blob))
        if payload[:4] in (b"##DT", b"##DZ"):
            blob.extend(payload)
        else:
            blob.extend(make_dt_block(payload))
    dl_addr = len(blob)
    blob.extend(make_dl_block(0, dt_addrs))
    if wrap_hl:
        hl_addr = len(blob)
        blob.extend(make_hl_block(dl_addr))
        return bytes(blob), hl_addr
    return bytes(blob), dl_addr


def cyclic_dl_file() -> Tuple[bytes, int]:
    """DL whose next link points back to itself (cycle guard test)."""
    blob = bytearray(b"\x00" * 8)
    payload = make_dt_block(b"\x01\x02\x03\x04")
    dt_addr = len(blob)
    blob.extend(payload)
    dl_addr = len(blob)
    dl = make_dl_block(dl_addr, [dt_addr])  # next -> self
    blob.extend(dl)
    return bytes(blob), dl_addr


def bytes_io_at(data: bytes, offset: int = 0) -> io.BytesIO:
    """Return a seekable buffer positioned at ``offset``."""
    bio = io.BytesIO(data)
    bio.seek(offset)
    return bio


def make_unknown_block(payload: bytes = b"\x00") -> bytes:
    """Build a non-DT/DZ/DL block for parse_subblocks fallback tests."""
    length = 24 + len(payload)
    return (
        b"##XX"
        + b"\x00" * 4
        + struct.pack("<Q", length)
        + struct.pack("<Q", 0)
        + payload
    )


def make_cc_block(
    cc_type: int = 1,
    cc_val_count: int = 2,
    params: tuple = (0.0, 1.0),
    link_count: int = 0,
) -> bytes:
    """Build a minimal ##CC block at offset 0 when laid at start of buffer."""
    header = (
        struct.pack("<B", cc_type)
        + struct.pack("<B", 0)
        + struct.pack("<H", 0)
        + struct.pack("<H", 0)
        + struct.pack("<H", cc_val_count)
        + struct.pack("<d", 0.0)
        + struct.pack("<d", 1.0)
    )
    if cc_val_count > 0:
        header += struct.pack(f"<{cc_val_count}d", *params[:cc_val_count])
    length = 24 + len(header)
    return (
        b"##CC"
        + b"\x00" * 4
        + struct.pack("<Q", length)
        + struct.pack("<Q", link_count)
        + header
    )
