# Known limitations

`impulse_ds.mdf` is **experimental** and under active development. It does **not**
yet fully implement the [ASAM MDF4](https://www.asam.net/standards/detail/mdf/)
specification. Some block types, encodings, compression modes, and edge cases may
be missing or behave differently than reference tools. Validate outputs against
your files before relying on this in production workflows.

**MDF4 only.** Files must have an `MDF` identification block and an `##HD` header
at offset 64. **MDF3** (and other legacy layouts) are not supported.

**Numeric-first output.** Signal values are decoded to a single `double` / `float`
column. String, byte-array, MIME, and complex channels are not represented in the
output schema.

---

## Backlog (feature gaps)

| area | severity | status |
| ---- | -------- | ------ |
| **VLSD channels** (`cn_type = 1`) — variable-length signals store an offset in the fixed record; the `##SD` payload linked from CN is not followed. Channels currently emit NaN. String/byte output would need schema changes. | MEDIUM | not implemented |
| **MLSD channels** (`cn_type = 5`) — maximum-length data lists are not decoded; bytes in the record are interpreted as fixed-width numeric data. | MEDIUM | not implemented |
| **CC type 3 (algebraic / formula)** — formula text in `cc_ref[0]` (`##TX`) is not read or evaluated; `apply_cc_conversion` has no handler for type 3. | LOW–MEDIUM | not implemented |
| **CC types 7–10 (text conversions)** — require `##TX` / `cc_ref` resolution. Unsupported by design for the numeric-only `value` column; `_parse_cc_block` returns `(-1, ())` for `cc_type > 6`. | LOW | not implemented (by design) |
| **CN composition** — the CN composition link (link 1) is not resolved; composite / array channels are not expanded. | MEDIUM | not implemented |
| **CN virtual data** (`cn_type = 6`) — not synthesized from other channels; record bytes are decoded as if the channel were fixed-length. | MEDIUM | not implemented |
| **CN sync channels** (`cn_type = 4`) — included in `mdf_signals` like ordinary signals rather than used as a time/sync axis. | LOW | not implemented |
| **Source information (`##SI`)** — `si_source` CN links are ignored; bus/protocol metadata is not surfaced. | LOW | not implemented |
| **Attachments** — `cn_attachment_count` is read for layout only; `##AT` blocks are not loaded. | LOW | not implemented |
| **Events / global metadata** — `##EV`, `##FH`, `##CH`, and other non-DG block types outside the HD→DG→CG→CN walk are not parsed. | LOW | not implemented |

**Unsorted DGs:** reads filter interleaved records by `record_id` before decode
(`filter_unsorted_records` in `mdf_decode.py`). Stripe mode concatenates
sub-blocks, then filters once per channel group.

---

## CC (`##CC`) block fields not used

When parsing channel conversions (`_parse_cc_block` in `mdf4_reader.py`), only
`cc_type` and the inline `cc_val_count` double parameters are returned. The
following CC header fields are read to advance the file pointer but **not applied**
to decoded values:

| field | notes |
| ----- | ----- |
| `cc_precision` | physical-value decimal places — ignored |
| `cc_flags` | status / validity flags — ignored |
| `cc_ref_count` | number of `cc_ref` links — ignored |
| `cc_phy_range_min` / `cc_phy_range_max` | expected physical range — not used for clamping or validation |

Additionally:

- **CC reference links** (name, unit, comment, inverse CC, `cc_ref` TX blocks for
  formulas and text tables) are skipped entirely; only inline numeric parameters
  are used for types 0–6.
- **Inverse CC** — the inverse-conversion link is not followed.

---

## CN (`##CN`) block fields not used

CN layout fields are read in spec order during `scan_metadata`, but only
`cn_type`, `cn_data_type`, offsets, `cn_bit_count`, `cn_flags`, and
`cn_invalid_bit_pos` drive decoding. These fields are **not used** downstream:

| field | notes |
| ----- | ----- |
| `cn_sync_type` | sync relationship to master — ignored |
| `cn_precision` | display precision — ignored |
| `cn_attachment_count` | attachment list size — ignored |
| `cn_val_range_min` / `cn_val_range_max` | value range — not used for validation |
| `cn_limit_min` / `cn_limit_max` | soft limits — ignored |
| `cn_limit_ext_min` / `cn_limit_ext_max` | extended limits — ignored |

**CG-level fields** `cg_flags` and `cg_path_separator` are likewise read for layout
only and not interpreted.

---

## Data types

`convert_values` (`mdf_decode.py`) fully decodes little- and big-endian integer
and float types (types 0–5) for common bit widths. All other `cn_data_type` values
fall through to **zeros** (or NaN for VLSD):

| `cn_data_type` | name | behaviour |
| -------------- | ---- | --------- |
| 6–9 | string (Latin / UTF-8 / UTF-16) | zeros emitted |
| 10 | byte array | zeros emitted |
| 11–12 | MIME sample / stream | zeros emitted |
| 13–14 | CANopen date / time | zeros emitted |
| 15–16 | complex LE / BE | zeros emitted |

Unsupported **float bit widths** (e.g. float16) within types 4–5 also produce zeros.

**Endianness / alignment:** fast strided decode paths in `extract_signal` and
`extract_timestamps` are implemented for **little-endian, byte-aligned** fields.
Big-endian and unaligned (`bit_offset > 0`) types use the slower generic path in
`convert_values`.

---

## Data blocks and I/O

Supported payload containers: `##DT`, `##DZ` (zlib deflate; `zip_type` 0 = plain,
1 = transposed deflate), `##DL` / `##HL` chains.

| gap | notes |
| --- | ----- |
| **Unknown / future block types** at the DG data link | `read_raw_data` falls back to reading `record_size * sample_count` bytes from offset 24 with no structure validation. |
| **Non-zlib `##DZ` compression** | only zlib (`zip_type` 0/1) is handled; other MDF compression identifiers are not implemented. |
| **Malformed DL chains** | cyclic DL links stop traversal; truncated chains may yield partial data without error. |

---

## Semantic / API limitations

| gap | notes |
| --- | ----- |
| **One master per group** | `scan_channels_organized` keeps the last `CN_TYPE_MASTER` / `CN_TYPE_VIRTUAL_MASTER` per `group_idx`; files with multiple masters per group are not modeled. |
| **Fixed CN link indices** | name, CC, unit, and comment addresses assume the standard MDF4 link order; variant link counts / orderings may mis-resolve metadata. |
| **Absolute time precision** | `read_header_start_epoch_seconds` documents float64 epoch seconds (~0.3 µs resolution at current epoch); HD nanosecond start time is not preserved bit-for-bit in outputs. |
| **Invalidation** | per-sample invalidation bits are applied when `CN_FLAG_INVALIDATION_PRESENT` is set; other CN/CG invalidation modes may differ from reference tools. |
