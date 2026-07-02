"""
Low-level MDF4 binary reader for extracting channel data without loading
the entire file into memory. Designed to be used inside Spark workers
where each task reads only the channels assigned to it.

MDF4 format reference: ASAM MDF v4.x specification.
Key blocks: HD (Header), DG (Data Group), CG (Channel Group), CN (Channel),
            DT/DZ/DL (Data blocks).
"""

import struct
from datetime import datetime, timezone
import numpy as np
from typing import BinaryIO, List, Tuple, Dict, Optional
from dataclasses import dataclass


# MDF4 block IDs
BLOCK_ID_HD = b"##HD"
BLOCK_ID_DG = b"##DG"
BLOCK_ID_CG = b"##CG"
BLOCK_ID_CN = b"##CN"
BLOCK_ID_DT = b"##DT"
BLOCK_ID_DZ = b"##DZ"
BLOCK_ID_DL = b"##DL"
BLOCK_ID_SD = b"##SD"
BLOCK_ID_TX = b"##TX"
BLOCK_ID_CC = b"##CC"
BLOCK_ID_SI = b"##SI"

# MDF4 data types for CN blocks
CN_DATA_TYPE_UNSIGNED_INT_LE = 0
CN_DATA_TYPE_UNSIGNED_INT_BE = 1
CN_DATA_TYPE_SIGNED_INT_LE = 2
CN_DATA_TYPE_SIGNED_INT_BE = 3
CN_DATA_TYPE_FLOAT_LE = 4
CN_DATA_TYPE_FLOAT_BE = 5
CN_DATA_TYPE_STRING_LATIN = 6
CN_DATA_TYPE_STRING_UTF8 = 7
CN_DATA_TYPE_STRING_UTF16_LE = 8
CN_DATA_TYPE_STRING_UTF16_BE = 9
CN_DATA_TYPE_BYTE_ARRAY = 10
CN_DATA_TYPE_MIME_SAMPLE = 11
CN_DATA_TYPE_MIME_STREAM = 12
CN_DATA_TYPE_CANOPEN_DATE = 13
CN_DATA_TYPE_CANOPEN_TIME = 14
CN_DATA_TYPE_COMPLEX_LE = 15
CN_DATA_TYPE_COMPLEX_BE = 16

# Channel types
CN_TYPE_FIXED = 0
CN_TYPE_VLSD = 1
CN_TYPE_MASTER = 2
CN_TYPE_VIRTUAL_MASTER = 3
CN_TYPE_SYNC = 4
CN_TYPE_MLSD = 5
CN_TYPE_VIRTUAL_DATA = 6

# CN flags for invalidation
CN_FLAG_ALL_INVALID = 1
CN_FLAG_INVALIDATION_PRESENT = 1 << 1


@dataclass
class ChannelInfo:
    """Metadata for a single channel within an MDF4 file."""
    group_idx: int
    channel_idx: int
    channel_name: str
    unit: str
    sample_count: int
    data_type: int
    bit_offset: int
    byte_offset: int
    bit_count: int
    channel_type: int
    # Offsets for direct binary access
    cn_block_addr: int
    cg_block_addr: int
    dg_block_addr: int
    data_block_addr: int
    record_size: int  # total bytes per record in this channel group (incl rec_id)
    # Invalidation bit handling
    cn_flags: int = 0
    invalidation_bit_pos: int = 0
    invalidation_bytes: int = 0  # number of invalidation bytes per record in this CG
    data_bytes: int = 0  # data bytes per record (record_size - invalidation_bytes)
    # CC (Channel Conversion) block
    cc_type: int = -1  # -1 = no conversion, 0 = identity, 1 = linear, etc.
    cc_params: tuple = ()
    # Record ID for unsorted data groups
    rec_id_size: int = 0  # 0=sorted, 1/2/4/8=unsorted
    record_id: int = 0  # cg_record_id for this channel group
    # Raw cn_md_comment block text (##MD XML or ##TX), or "" if none
    md_comment: str = ""


@dataclass
class DataGroupInfo:
    """Metadata for a data group."""
    address: int
    data_block_addr: int
    channel_groups: List["ChannelGroupInfo"]


@dataclass
class ChannelGroupInfo:
    """Metadata for a channel group."""
    address: int
    record_id: int
    cycle_count: int
    data_bytes: int
    invalidation_bytes: int
    channels: List[ChannelInfo]


class MDF4Reader:
    """
    Reads MDF4 file structure and extracts raw signal data using
    direct binary access with minimal memory footprint.
    """

    def __init__(self, file_path: str = None, file_bytes: "bytes" = None):
        """Read from a path, or from an in-memory buffer (file_bytes) so a caller
        that already loaded the whole file once (Design B planning) can scan +
        build the block map from RAM without re-reading the volume."""
        self.file_path = file_path
        self._file_bytes = file_bytes
        self._data_groups: Optional[List[DataGroupInfo]] = None
        self._text_cache: Dict[int, str] = {}  # address -> TX/MD text (immutable per file)

    def _open(self):
        """Context manager yielding a seekable binary source (in-RAM buffer if
        file_bytes was provided, else the file on disk)."""
        import contextlib
        import io
        if self._file_bytes is not None:
            @contextlib.contextmanager
            def _buf():
                yield io.BytesIO(self._file_bytes)
            return _buf()
        return open(self.file_path, "rb")

    def _read_hd_start_utc_ns(self) -> Optional[int]:
        """Read the measurement start time from the HD block as UTC nanoseconds
        since the Unix epoch (full precision), or None if not set.

        The HD block stores the start time in nanoseconds plus, when the local
        time flag and offsets-valid flag are both set, a timezone+DST offset in
        minutes; that offset is subtracted to normalize to UTC.
        """
        with self._open() as f:
            f.seek(0)
            sig = f.read(8)
            if not sig.startswith(b"MDF"):
                raise ValueError(f"Not a valid MDF file: {self.file_path}")

            # ID block is 64 bytes; HD block is fixed at offset 64.
            f.seek(64)
            block_id = f.read(4)
            if block_id != BLOCK_ID_HD:
                raise ValueError(f"Expected HD block at offset 64, got {block_id}")
            f.read(4)  # reserved
            f.read(8)  # block length
            hd_link_count = struct.unpack("<Q", f.read(8))[0]
            f.read(8 * hd_link_count)  # skip HD links

            hd_start_time_ns = struct.unpack("<Q", f.read(8))[0]
            hd_tz_offset_min = struct.unpack("<h", f.read(2))[0]
            hd_dst_offset_min = struct.unpack("<h", f.read(2))[0]
            hd_time_flags = struct.unpack("<B", f.read(1))[0]
            if hd_start_time_ns == 0:
                return None

            # bit 0: local time flag; bit 1: timezone/DST offsets valid.
            is_local_time = bool(hd_time_flags & 0x01)
            offsets_valid = bool(hd_time_flags & 0x02)
            utc_ns = hd_start_time_ns
            if is_local_time and offsets_valid:
                total_offset_min = hd_tz_offset_min + hd_dst_offset_min
                utc_ns -= total_offset_min * 60 * 1_000_000_000
            return utc_ns

    def read_header_datetime(self) -> Optional[datetime]:
        """
        Read measurement start datetime from the MDF4 HD block.

        Returns:
            Naive UTC datetime if present, otherwise None.
        """
        utc_ns = self._read_hd_start_utc_ns()
        if utc_ns is None:
            return None
        seconds, nanoseconds = divmod(utc_ns, 1_000_000_000)
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc).replace(
            microsecond=nanoseconds // 1000,
        )
        return dt.replace(tzinfo=None)

    def read_header_start_epoch_seconds(self) -> Optional[float]:
        """Measurement start time as Unix epoch seconds (UTC), as a float with
        the HD block's sub-second precision (not rounded to whole seconds), or
        None if not set. Used to make signal timestamps absolute.

        Note: at present-day epoch magnitudes (~1.8e9 s) a float64 resolves to
        ~0.3 us, so absolute times keep sub-microsecond — not full nanosecond —
        precision.
        """
        utc_ns = self._read_hd_start_utc_ns()
        return None if utc_ns is None else utc_ns / 1e9

    def _read_text_block(self, f: BinaryIO, address: int) -> str:
        """Read a TX or MD text block, returning the string content. Cached by
        address (text blocks are immutable, and an MD comment is often shared by
        many channels — avoids re-reading the same block per channel)."""
        if address == 0:
            return ""
        cached = self._text_cache.get(address)
        if cached is not None:
            return cached
        f.seek(address)
        block_id = f.read(4)
        if block_id not in (BLOCK_ID_TX, b"##MD"):
            self._text_cache[address] = ""
            return ""
        f.read(4)  # reserved
        block_len = struct.unpack("<Q", f.read(8))[0]
        f.read(8)  # link count
        # Text starts after the 24-byte header
        text_len = block_len - 24
        if text_len <= 0:
            self._text_cache[address] = ""
            return ""
        raw = f.read(text_len)
        # Strip null terminators
        null_pos = raw.find(b"\x00")
        if null_pos >= 0:
            raw = raw[:null_pos]
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("latin-1")
        self._text_cache[address] = text
        return text

    @staticmethod
    def _parse_cc_block(f: BinaryIO, cc_addr: int) -> Tuple[int, tuple]:
        """
        Parse a CC (Channel Conversion) block and return (cc_type, cc_params).

        Returns (-1, ()) if the block is invalid or address is 0.
        """
        if cc_addr == 0:
            return -1, ()
        f.seek(cc_addr)
        block_id = f.read(4)
        if block_id != BLOCK_ID_CC:
            return -1, ()
        f.read(4)  # reserved
        block_len = struct.unpack("<Q", f.read(8))[0]
        link_count = struct.unpack("<Q", f.read(8))[0]

        # Skip links (name, unit, comment, inverse_cc, ref_params...)
        f.read(8 * link_count)

        # CC data section. Each field is read in spec order to advance the file
        # position to cc_val_count; the fields named but not used downstream
        # (precision, flags, ref_count, phy_range_*) are kept as named reads to
        # document the block layout and keep the offsets self-evident.
        cc_type = struct.unpack("<B", f.read(1))[0]
        cc_precision = struct.unpack("<B", f.read(1))[0]      # noqa: F841 (layout)
        cc_flags = struct.unpack("<H", f.read(2))[0]          # noqa: F841 (layout)
        cc_ref_count = struct.unpack("<H", f.read(2))[0]      # noqa: F841 (layout)
        cc_val_count = struct.unpack("<H", f.read(2))[0]
        cc_phy_range_min = struct.unpack("<d", f.read(8))[0]  # noqa: F841 (layout)
        cc_phy_range_max = struct.unpack("<d", f.read(8))[0]  # noqa: F841 (layout)

        # Read value parameters
        if cc_val_count > 0:
            cc_params = struct.unpack(f"<{cc_val_count}d", f.read(8 * cc_val_count))
        else:
            cc_params = ()

        # Only support numeric conversion types (0-6)
        if cc_type > 6:
            return -1, ()

        return cc_type, cc_params

    def scan_metadata(self) -> List[ChannelInfo]:
        """
        Scan the MDF4 file to extract all channel metadata without
        reading actual signal data. Returns list of ChannelInfo objects.
        """
        channels = []
        with self._open() as f:
            # Verify MDF4 signature
            f.seek(0)
            sig = f.read(8)
            if not sig.startswith(b"MDF"):
                raise ValueError(f"Not a valid MDF file: {self.file_path}")

            # Read identification block to get header address
            # ID block is 64 bytes, HD block starts at offset 64
            hd_addr = 64

            # Read HD block
            f.seek(hd_addr)
            block_id = f.read(4)
            if block_id != BLOCK_ID_HD:
                raise ValueError(f"Expected HD block at offset 64, got {block_id}")
            f.read(4)  # reserved
            hd_block_len = struct.unpack("<Q", f.read(8))[0]
            hd_link_count = struct.unpack("<Q", f.read(8))[0]

            # HD links: first link is to first DG block
            hd_links = struct.unpack(f"<{hd_link_count}Q", f.read(8 * hd_link_count))
            first_dg_addr = hd_links[0]

            # Traverse DG linked list
            group_idx = 0
            dg_addr = first_dg_addr
            while dg_addr != 0:
                f.seek(dg_addr)
                block_id = f.read(4)
                if block_id != BLOCK_ID_DG:
                    break
                f.read(4)  # reserved
                dg_block_len = struct.unpack("<Q", f.read(8))[0]
                dg_link_count = struct.unpack("<Q", f.read(8))[0]

                # DG links: [next_dg, first_cg, data_block, comment]
                dg_links = struct.unpack(f"<{dg_link_count}Q", f.read(8 * dg_link_count))
                next_dg_addr = dg_links[0]
                first_cg_addr = dg_links[1]
                data_block_addr = dg_links[2]

                # DG data section
                dg_rec_id_size = struct.unpack("<B", f.read(1))[0]
                # skip remaining 7 reserved bytes
                f.read(7)

                # Traverse CG linked list
                cg_addr = first_cg_addr
                while cg_addr != 0:
                    f.seek(cg_addr)
                    block_id = f.read(4)
                    if block_id != BLOCK_ID_CG:
                        break
                    f.read(4)  # reserved
                    cg_block_len = struct.unpack("<Q", f.read(8))[0]
                    cg_link_count = struct.unpack("<Q", f.read(8))[0]

                    # CG links: [next_cg, first_cn, acq_name, acq_source, ...]
                    cg_links = struct.unpack(f"<{cg_link_count}Q", f.read(8 * cg_link_count))
                    next_cg_addr = cg_links[0]
                    first_cn_addr = cg_links[1]

                    # CG data section
                    cg_record_id = struct.unpack("<Q", f.read(8))[0]
                    cg_cycle_count = struct.unpack("<Q", f.read(8))[0]
                    cg_flags = struct.unpack("<H", f.read(2))[0]
                    cg_path_separator = struct.unpack("<H", f.read(2))[0]
                    f.read(4)  # reserved
                    cg_data_bytes = struct.unpack("<I", f.read(4))[0]
                    cg_invalidation_bytes = struct.unpack("<I", f.read(4))[0]

                    record_size = dg_rec_id_size + cg_data_bytes + cg_invalidation_bytes

                    # Traverse CN linked list
                    channel_idx = 0
                    cn_addr = first_cn_addr
                    while cn_addr != 0:
                        f.seek(cn_addr)
                        block_id = f.read(4)
                        if block_id != BLOCK_ID_CN:
                            break
                        f.read(4)  # reserved
                        cn_block_len = struct.unpack("<Q", f.read(8))[0]
                        cn_link_count = struct.unpack("<Q", f.read(8))[0]

                        # CN links vary by version; first links are standard:
                        # [next_cn, composition, tx_name, si_source, cc_conversion, data/signal_data, unit, comment, ...]
                        cn_links = struct.unpack(f"<{cn_link_count}Q", f.read(8 * cn_link_count))
                        next_cn_addr = cn_links[0]
                        cn_name_addr = cn_links[1] if cn_link_count > 1 else 0
                        # In MDF4, link order is: next_cn, composition, tx_name, si_source, cc_conversion, data, unit, comment
                        # But link count varies; for standard channels:
                        # link 0: next CN
                        # link 1: composition
                        # link 2: TX name
                        # link 3: SI source
                        # link 4: CC conversion
                        # link 5: signal data (for VLSD)
                        # link 6: TX unit
                        # link 7: TX/MD comment
                        tx_name_addr = cn_links[2] if cn_link_count > 2 else 0
                        cc_addr = cn_links[4] if cn_link_count > 4 else 0
                        unit_addr = cn_links[6] if cn_link_count > 6 else 0
                        comment_addr = cn_links[7] if cn_link_count > 7 else 0

                        # CN data section (after links). The fields the converter
                        # actually uses are cn_type, cn_data_type, cn_bit_offset,
                        # cn_byte_offset, cn_bit_count, cn_flags, cn_invalid_bit_pos.
                        # The rest (sync_type, precision, attachment_count, value/
                        # limit ranges) are read in spec order purely to advance the
                        # file position; they are named to document the block layout.
                        cn_type = struct.unpack("<B", f.read(1))[0]
                        cn_sync_type = struct.unpack("<B", f.read(1))[0]      # noqa: F841 (layout)
                        cn_data_type = struct.unpack("<B", f.read(1))[0]
                        cn_bit_offset = struct.unpack("<B", f.read(1))[0]
                        cn_byte_offset = struct.unpack("<I", f.read(4))[0]
                        cn_bit_count = struct.unpack("<I", f.read(4))[0]
                        cn_flags = struct.unpack("<I", f.read(4))[0]
                        cn_invalid_bit_pos = struct.unpack("<I", f.read(4))[0]
                        cn_precision = struct.unpack("<B", f.read(1))[0]     # noqa: F841 (layout)
                        f.read(1)  # reserved
                        cn_attachment_count = struct.unpack("<H", f.read(2))[0]  # noqa: F841 (layout)
                        cn_val_range_min = struct.unpack("<d", f.read(8))[0]  # noqa: F841 (layout)
                        cn_val_range_max = struct.unpack("<d", f.read(8))[0]  # noqa: F841 (layout)
                        cn_limit_min = struct.unpack("<d", f.read(8))[0]      # noqa: F841 (layout)
                        cn_limit_max = struct.unpack("<d", f.read(8))[0]      # noqa: F841 (layout)
                        cn_limit_ext_min = struct.unpack("<d", f.read(8))[0]  # noqa: F841 (layout)
                        cn_limit_ext_max = struct.unpack("<d", f.read(8))[0]  # noqa: F841 (layout)

                        # Read channel name, unit, and the cn_md_comment header
                        # (##MD XML or ##TX) — link 7.
                        channel_name = self._read_text_block(f, tx_name_addr)
                        unit = self._read_text_block(f, unit_addr)
                        md_comment = self._read_text_block(f, comment_addr)

                        # Parse CC conversion block
                        cc_type, cc_params = self._parse_cc_block(f, cc_addr)

                        channels.append(ChannelInfo(
                            group_idx=group_idx,
                            channel_idx=channel_idx,
                            channel_name=channel_name,
                            unit=unit,
                            sample_count=cg_cycle_count,
                            data_type=cn_data_type,
                            bit_offset=cn_bit_offset,
                            byte_offset=dg_rec_id_size + cn_byte_offset,
                            bit_count=cn_bit_count,
                            channel_type=cn_type,
                            cn_block_addr=cn_addr,
                            cg_block_addr=cg_addr,
                            dg_block_addr=dg_addr,
                            data_block_addr=data_block_addr,
                            record_size=record_size,
                            cn_flags=cn_flags,
                            invalidation_bit_pos=cn_invalid_bit_pos,
                            invalidation_bytes=cg_invalidation_bytes,
                            data_bytes=dg_rec_id_size + cg_data_bytes,
                            cc_type=cc_type,
                            cc_params=cc_params,
                            rec_id_size=dg_rec_id_size,
                            record_id=cg_record_id,
                            md_comment=md_comment,
                        ))

                        channel_idx += 1
                        cn_addr = next_cn_addr

                    cg_addr = next_cg_addr
                    group_idx += 1

                dg_addr = next_dg_addr

        return channels

    @staticmethod
    def read_channel_data(
        file_path: str,
        data_block_addr: int,
        record_size: int,
        byte_offset: int,
        bit_offset: int,
        bit_count: int,
        data_type: int,
        channel_type: int,
        sample_count: int,
        cn_flags: int = 0,
        invalidation_bit_pos: int = 0,
        invalidation_bytes: int = 0,
        data_bytes: int = 0,
        cc_type: int = -1,
        cc_params: tuple = (),
        f: Optional[BinaryIO] = None,
    ) -> np.ndarray:
        """
        Read raw signal data for a single channel directly from the binary file.
        Returns values as a numpy float64 array with CC conversion applied.
        Invalid samples (per the MDF4 invalidation bit mechanism) are marked with np.nan.

        If an open file handle `f` is provided, it will be used instead of
        opening the file again.

        This delegates to the executor-side decode functions in udf_helpers so
        that this reference API exercises the exact code path used in Spark.
        """
        from .udf_helpers import read_raw_data, extract_signal

        ch_spec = {
            "channel_type": channel_type,
            "data_type": data_type,
            "bit_count": bit_count,
            "byte_offset": byte_offset,
            "bit_offset": bit_offset,
            "record_size": record_size,
            "cn_flags": cn_flags,
            "invalidation_bit_pos": invalidation_bit_pos,
            "invalidation_bytes": invalidation_bytes,
            "data_bytes": data_bytes,
            "cc_type": cc_type,
            "cc_params": list(cc_params) if cc_params else [],
        }

        def _extract(raw_data):
            if not raw_data or record_size <= 0:
                return np.array([], dtype=np.float64)
            values = extract_signal(raw_data, record_size, ch_spec)
            if values is None:
                return np.array([], dtype=np.float64)
            return values

        if f is not None:
            raw_data = read_raw_data(f, data_block_addr, record_size, sample_count)
            return _extract(raw_data)

        with open(file_path, "rb") as fh:
            raw_data = read_raw_data(fh, data_block_addr, record_size, sample_count)

        return _extract(raw_data)

    @staticmethod
    def read_channel_pair(
        file_path: str,
        master_info: dict,
        signal_info: dict,
        sample_count: int,
        f: Optional[BinaryIO] = None,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Read both master (time) and signal channel data for a channel group.
        Returns (timestamps, values) both as float64 arrays.
        Invalid signal samples are marked with np.nan based on invalidation bits.

        If an open file handle `f` is provided, it will be used instead of
        opening the file again.
        """
        from .udf_helpers import (
            read_raw_data, extract_signal, extract_timestamps,
        )

        def _do_read(fh: BinaryIO) -> Tuple[np.ndarray, np.ndarray]:
            data_block_addr = signal_info["data_block_addr"]
            record_size = signal_info["record_size"]

            # Read the raw data block once (shared between master and signal)
            raw_data = read_raw_data(fh, data_block_addr, record_size, sample_count)

            actual_samples = len(raw_data) // record_size if record_size else 0
            if master_info:
                timestamps = extract_timestamps(raw_data, record_size, master_info)
            else:
                timestamps = np.arange(actual_samples, dtype=np.float64)

            values = extract_signal(raw_data, record_size, signal_info)
            if values is None:
                values = np.full(len(timestamps), np.nan, dtype=np.float64)

            return timestamps, values

        if f is not None:
            return _do_read(f)

        with open(file_path, "rb") as fh:
            return _do_read(fh)


    def scan_channels_organized(self) -> dict:
        """
        Scan metadata and return channels organized into masters and signals.

        Returns dict with:
            "master_channels": {group_idx: ChannelInfo} - one master per group
            "signal_channels": [ChannelInfo] - all non-master channels
            "channel_id_map": {(group_idx, channel_idx): channel_id} - sequential IDs for signals
        """
        all_channels = self.scan_metadata()
        master_channels = {}
        signal_channels = []
        channel_id_map = {}
        channel_id = 0

        for ch in all_channels:
            if ch.channel_type in (CN_TYPE_MASTER, CN_TYPE_VIRTUAL_MASTER):
                master_channels[ch.group_idx] = ch
            else:
                channel_id_map[(ch.group_idx, ch.channel_idx)] = channel_id
                signal_channels.append(ch)
                channel_id += 1

        return {
            "master_channels": master_channels,
            "signal_channels": signal_channels,
            "channel_id_map": channel_id_map,
        }

    @staticmethod
    def channel_to_dict(ch: "ChannelInfo") -> dict:
        """Convert a ChannelInfo to a serializable dict suitable for bin packing."""
        return {
            "group_idx": ch.group_idx,
            "channel_idx": ch.channel_idx,
            "sample_count": ch.sample_count,
            "channel_name": ch.channel_name,
            "unit": ch.unit,
            "data_type": ch.data_type,
            "bit_offset": ch.bit_offset,
            "byte_offset": ch.byte_offset,
            "bit_count": ch.bit_count,
            "channel_type": ch.channel_type,
            "data_block_addr": ch.data_block_addr,
            "record_size": ch.record_size,
            "cn_flags": ch.cn_flags,
            "invalidation_bit_pos": ch.invalidation_bit_pos,
            "invalidation_bytes": ch.invalidation_bytes,
            "data_bytes": ch.data_bytes,
            "cc_type": ch.cc_type,
            "cc_params": list(ch.cc_params) if ch.cc_params else [],
        }

    @staticmethod
    def master_to_dict(ch: "ChannelInfo") -> dict:
        """Convert a master ChannelInfo to a serializable dict for timestamp extraction."""
        return {
            "byte_offset": ch.byte_offset,
            "bit_offset": ch.bit_offset,
            "bit_count": ch.bit_count,
            "data_type": ch.data_type,
            "channel_type": ch.channel_type,
            "cc_type": ch.cc_type,
            "cc_params": list(ch.cc_params) if ch.cc_params else [],
        }
