"""
Partition planning for distributing MDF4 channel data across Spark tasks.

`plan_partitions` is the primary planner: it sizes partitions by estimated
output rows (record-range splits for big groups, channel-subset splits for very
wide groups, coalescing for many small groups) using only scan metadata — no
file I/O. `plan_stripes_for_file` is the alternative byte-offset "stripe"
planner, and `plan_master_partitions` plans the per-group master time base.
"""

from .mdf4_reader import MDF4Reader


def plan_partitions(
    file_path: str,
    master_channels: dict,
    signal_channels: list,
    channel_id_map: dict,
    target_partition_mb: float = 64.0,
    channel_threshold: int = 256,
    time_dtype: str = "float64",
    value_dtype: str = "float64",
    run_length_encoding: bool = False,
    max_groups_per_partition: int = 64,
    time_offset: float = 0.0,
    unsorted_dg_ctx: dict = None,
) -> list[dict]:
    """
    Plan Spark partitions for one MDF file, decoupling parallelism from channel
    count so executors stay saturated and a single dominant channel group no
    longer becomes one serial straggler.

    Strategy per channel group, sized to ~target rows of output (16 bytes/row),
    so parallelism tracks data volume, not channel count — using ONLY metadata
    already in hand (no per-group file reads):
      - Few channels (<= channel_threshold): split by EVEN RECORD RANGE so many
        tasks process disjoint row ranges of the group. At read time DT blocks
        seek to the range, DL/HL blocks decompress only the overlapping
        sub-blocks (read_data_list_range), and a standalone DZ is decompressed
        and sliced. Adjacent DL partitions share at most one boundary sub-block
        (negligible redundant decompression).
      - Wide groups (> channel_threshold channels): split by CHANNEL SUBSET
        (full rows) to avoid duplicating a huge channel list across slices.
      - Small groups whose whole output fits one partition are COALESCED with
        neighbouring small groups into a shared partition (see below), so that a
        file with thousands of tiny groups does not produce thousands of tiny
        tasks (per-task scheduling + Python round-trip overhead dominated, while
        a few big groups became stragglers — a severe load skew).

    Coalescing: small groups are packed, in file order (ascending data block
    address, for read locality), into a shared spec until EITHER the combined
    output reaches ~target_rows OR the spec already holds
    max_groups_per_partition groups. The second bound matters because each group
    in a coalesced spec is a separate (often scattered) block read on the
    executor; without it, a spec of thousands of tiny groups would just relocate
    the straggler into one task doing thousands of latency-bound reads. The
    executor (convert_spec_to_arrow_batches) already loops over the distinct
    data block addresses within a spec, so a multi-group spec needs no special
    handling there.

    Returns spec dicts consumed by udf_helpers.convert_spec_to_arrow_batches
    (each: file_path, channels, and optional row_start/row_end).

    Args use scan ChannelInfo objects (master_channels: {group_idx: ChannelInfo},
    signal_channels: [ChannelInfo], channel_id_map: {(group_idx, channel_idx): id}).
    """
    target_rows = max(1, int(target_partition_mb * 1024 * 1024 / 16))
    ctx = unsorted_dg_ctx or {}

    groups: dict[int, list] = {}
    for ch in signal_channels:
        groups.setdefault(ch.group_idx, []).append(ch)

    def _master_info(group_idx):
        m = master_channels.get(group_idx)
        if not m:
            return None
        return MDF4Reader.master_to_dict(m, ctx)

    def _ch_dict(ch):
        d = MDF4Reader.channel_to_dict(ch, ctx)
        d["channel_id"] = channel_id_map.get((ch.group_idx, ch.channel_idx), -1)
        d["master_info"] = _master_info(ch.group_idx)
        return d

    def _spec(ch_dicts, r0=None, r1=None):
        s = {
            "file_path": file_path,
            "channels": ch_dicts,
            "time_dtype": time_dtype,
            "value_dtype": value_dtype,
            "run_length_encoding": run_length_encoding,
            "time_offset": time_offset,
        }
        if r0 is not None:
            s["row_start"] = r0
            s["row_end"] = r1
        return s

    # NOTE: this function performs NO file I/O. It is pure metadata, O(channels),
    # so it scales to tens of thousands of groups. (Earlier versions peeked the
    # block id per group and/or walked DL sub-block chains on the driver; on a
    # FUSE-mounted volume those are scattered cold reads ~10-25 ms each, i.e.
    # minutes for 30k+ groups — a planning blocker.) Block type is resolved at
    # READ time on executors instead (DT seek / DL sub-block range / DZ slice).
    specs: list[dict] = []
    # Open bin for coalescing small (single-partition) groups. Flushed when it
    # reaches ~target_rows of output or max_groups_per_partition groups.
    pending_ch: list[dict] = []
    pending_rows = 0
    pending_groups = 0

    def _flush_small():
        nonlocal pending_ch, pending_rows, pending_groups
        if pending_ch:
            specs.append(_spec(pending_ch))
            pending_ch = []
            pending_rows = 0
            pending_groups = 0

    for chans in sorted(groups.values(), key=lambda cs: cs[0].data_block_addr):
        c = len(chans)
        group_records = chans[0].sample_count
        if c == 0 or group_records == 0:
            continue
        ch_dicts = [_ch_dict(ch) for ch in chans]

        if c > channel_threshold:
            # Wide group: split by channel subset (no channel-list duplication).
            ch_per_part = max(1, target_rows // group_records)
            for i in range(0, c, ch_per_part):
                specs.append(_spec(ch_dicts[i : i + ch_per_part]))
        else:
            rows_per_part = max(1, target_rows // c)
            if rows_per_part >= group_records:
                # Small group (fits one partition): coalesce with neighbours.
                grp_rows = c * group_records
                if pending_ch and (
                    pending_rows + grp_rows > target_rows
                    or pending_groups >= max_groups_per_partition
                ):
                    _flush_small()
                pending_ch.extend(ch_dicts)
                pending_rows += grp_rows
                pending_groups += 1
            else:
                r = 0
                while r < group_records:
                    r1 = min(r + rows_per_part, group_records)
                    specs.append(_spec(ch_dicts, r, r1))
                    r = r1

    _flush_small()
    return specs


def plan_master_partitions(
    file_path: str,
    master_channels: dict,
    target_partition_mb: float = 64.0,
    time_dtype: str = "float64",
    max_groups_per_partition: int = 64,
    time_offset: float = 0.0,
    unsorted_dg_ctx: dict = None,
) -> list[dict]:
    """
    Plan Spark partitions for the MASTER channels of one MDF file — the time
    base of each acquisition group, one master per group, emitted as one row per
    ORIGINAL sample (file_uri, group_idx, timestamp). This is the companion of
    run-length-encoded signals: RLE keeps only [tstart, tend] intervals, so the
    original per-sample grid is recovered by joining a group's stored timestamps
    against the intervals.

    Same sizing strategy as plan_partitions (no file I/O): a group with more than
    ~target_rows samples is split by EVEN RECORD RANGE; smaller groups are
    COALESCED (in file order, bounded by max_groups_per_partition block reads per
    task) so thousands of tiny groups don't each become a task.

    Returns spec dicts consumed by udf_helpers.convert_master_spec_to_arrow_batches
    (each: file_path, time_dtype, masters[list], optional
    row_start/row_end). Groups without a master are simply absent from the input.
    """
    target_rows = max(1, int(target_partition_mb * 1024 * 1024 / 16))
    ctx = unsorted_dg_ctx or {}

    def _master_entry(group_idx, m):
        from .mdf_decode import unsorted_fields_from_ctx

        entry = {
            "group_idx": group_idx,
            "data_block_addr": m.data_block_addr,
            "record_size": m.record_size,
            "sample_count": m.sample_count,
            "master_info": MDF4Reader.master_to_dict(m, ctx),
        }
        entry.update(unsorted_fields_from_ctx(m.dg_block_addr, m.record_id, ctx))
        return entry

    def _spec(masters, r0=None, r1=None):
        s = {
            "file_path": file_path,
            "time_dtype": time_dtype,
            "masters": masters,
            "time_offset": time_offset,
        }
        if r0 is not None:
            s["row_start"] = r0
            s["row_end"] = r1
        return s

    specs: list[dict] = []
    pending: list[dict] = []
    pending_rows = 0

    def _flush():
        nonlocal pending, pending_rows
        if pending:
            specs.append(_spec(pending))
            pending = []
            pending_rows = 0

    for group_idx, m in master_channels.items():
        group_records = m.sample_count
        if group_records == 0:
            continue
        entry = _master_entry(group_idx, m)
        if group_records > target_rows:
            # Big group: split the master into even record ranges (one per spec).
            r = 0
            while r < group_records:
                r1 = min(r + target_rows, group_records)
                specs.append(_spec([entry], r, r1))
                r = r1
        else:
            # Small group: coalesce with neighbours toward target_rows, bounding
            # the number of (scattered) block reads per task.
            if pending and (
                pending_rows + group_records > target_rows
                or len(pending) >= max_groups_per_partition
            ):
                _flush()
            pending.append(entry)
            pending_rows += group_records

    _flush()
    return specs


def plan_stripes_for_file(
    file_path: str,
    target_partition_mb: float = 64.0,
    time_dtype: str = "float64",
    value_dtype: str = "float64",
    run_length_encoding: bool = False,
    time_offset: float = 0.0,
    stripe_target_mb: float = 128.0,
    gap_threshold_mb: float = 8.0,
    max_subblocks_per_stripe: int = 4096,
    file_bytes: bytes = None,
) -> list[dict]:
    """Design B planner: read the file ONCE (in RAM), build a sub-block map, and
    pack sub-blocks — sorted by file offset — into contiguous byte-offset STRIPES.

    Each stripe is bounded by: target_rows (output budget, from target_partition_mb,
    keeps Delta file sizing consistent), stripe_target_mb (compressed bytes read
    per task, memory/IO bound), a gap guard (don't read large non-data spans), and
    a sub-block-count cap. A stripe is decoded with one sequential read
    (udf_helpers.convert_stripe_spec_to_arrow_batches).

    Returns stripe spec dicts. file_bytes lets a caller that already loaded the
    file pass it in (avoids a re-read).
    """
    import io
    from .mdf4_reader import MDF4Reader
    from .udf_helpers import parse_subblocks

    if file_bytes is None:
        with open(file_path, "rb") as fh:
            file_bytes = fh.read()
    reader = MDF4Reader(file_bytes=file_bytes)
    organized = reader.scan_channels_organized()
    master_channels = organized["master_channels"]
    signal_channels = organized["signal_channels"]
    channel_id_map = organized["channel_id_map"]
    unsorted_dg_ctx = organized.get("unsorted_dg_ctx") or {}

    target_rows = max(1, int(target_partition_mb * 1024 * 1024 / 16))
    stripe_bytes_cap = int(stripe_target_mb * 1024 * 1024)
    gap_threshold = int(gap_threshold_mb * 1024 * 1024)

    def _minfo(group_idx):
        m = master_channels.get(group_idx)
        if not m:
            return None
        return MDF4Reader.master_to_dict(m, unsorted_dg_ctx)

    def _chd(ch):
        d = MDF4Reader.channel_to_dict(ch, unsorted_dg_ctx)
        d["channel_id"] = channel_id_map.get((ch.group_idx, ch.channel_idx), -1)
        return d

    bio = io.BytesIO(file_bytes)
    by_group: dict[int, list] = {}
    for ch in signal_channels:
        by_group.setdefault(ch.group_idx, []).append(ch)

    groups_meta = {}
    subblocks = []
    for group_idx, chans in by_group.items():
        ch0 = chans[0]
        rs = ch0.record_size
        sc = ch0.sample_count
        if rs == 0 or sc == 0:
            continue
        from .mdf_decode import unsorted_fields_from_ctx

        meta = {
            "group_idx": group_idx,
            "data_block_addr": ch0.data_block_addr,
            "record_size": rs,
            "master_info": _minfo(group_idx),
            "channels": [_chd(c) for c in chans],
            "n_channels": len(chans),
            "sample_count": sc,
        }
        meta.update(unsorted_fields_from_ctx(ch0.dg_block_addr, ch0.record_id, unsorted_dg_ctx))
        groups_meta[group_idx] = meta
        for soff, slen, rstart, rcount in parse_subblocks(bio, ch0.data_block_addr, rs, sc):
            if rcount <= 0 and slen <= 0:
                continue
            subblocks.append(
                {
                    "group_idx": group_idx,
                    "grp": ch0.data_block_addr,
                    "abs_off": soff,
                    "on_disk_len": slen,
                    "rec_start": rstart,
                    "rec_count": rcount,
                }
            )

    subblocks.sort(key=lambda s: s["abs_off"])

    specs = []
    cur = []
    cur_rows = cur_bytes = 0
    cur_lo = cur_hi = None
    cur_grps = set()

    def _flush():
        nonlocal cur, cur_rows, cur_bytes, cur_lo, cur_hi, cur_grps
        if cur:
            specs.append(
                {
                    "file_path": file_path,
                    "byte_start": cur_lo,
                    "byte_end": cur_hi,
                    "time_dtype": time_dtype,
                    "value_dtype": value_dtype,
                    "run_length_encoding": run_length_encoding,
                    "time_offset": time_offset,
                    "groups": {str(g): groups_meta[g] for g in cur_grps},
                    "subblocks": cur,
                }
            )
            cur = []
            cur_rows = cur_bytes = 0
            cur_lo = cur_hi = None
            cur_grps = set()

    for sb in subblocks:
        gidx = sb["group_idx"]
        rows = sb["rec_count"] * groups_meta[gidx]["n_channels"]
        gap = (sb["abs_off"] - cur_hi) if cur_hi is not None else 0
        if cur and (
            cur_rows + rows > target_rows
            or cur_bytes + sb["on_disk_len"] > stripe_bytes_cap
            or gap > gap_threshold
            or len(cur) >= max_subblocks_per_stripe
        ):
            _flush()
        cur.append(sb)
        cur_rows += rows
        cur_bytes += sb["on_disk_len"]
        cur_grps.add(gidx)
        end = sb["abs_off"] + sb["on_disk_len"]
        cur_lo = sb["abs_off"] if cur_lo is None else min(cur_lo, sb["abs_off"])
        cur_hi = end if cur_hi is None else max(cur_hi, end)

    _flush()
    return specs
