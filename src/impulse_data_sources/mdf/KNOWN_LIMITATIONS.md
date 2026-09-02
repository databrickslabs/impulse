# Known limitations

> 📖 **The full, maintained limitations reference is in the published docs:**
> <https://databrickslabs.github.io/impulse/docs/data_sources/mdf4#known-limitations>
> (source: [`docs/impulse/docs/data_sources/mdf4.md`](../../../docs/impulse/docs/data_sources/mdf4.md)).

`impulse_data_sources.mdf` is **experimental** and under active development. It does
**not** yet fully implement the
[ASAM MDF4](https://www.asam.net/standards/detail/mdf/) specification. Validate
outputs against your files before relying on this in production.

- **MDF4 only.** Files must have an `MDF` identification block and an `##HD` header
  at offset 64. MDF3 and other legacy layouts are not supported.
- **Numeric-first output.** Values decode to a single `double` / `float` column;
  string, byte-array, MIME, and complex channels are not represented.

See the published [Known limitations](https://databrickslabs.github.io/impulse/docs/data_sources/mdf4#known-limitations)
section for the full backlog (VLSD/MLSD/composition/virtual channels, CC and CN
fields not applied, unsupported data types, data-block/IO gaps, and semantic
limitations).
