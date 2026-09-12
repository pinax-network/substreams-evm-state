# Protocol fixture provenance

`cursors.json` contains public bstream cursor-format vectors for blocks 100–110
and steps 1/17. They were originally generated with StreamingFast's Go `opaque`
library at `v0.0.0-20210811180740-0c01d37ea308`; the original generator remains
in repository history before the Rust migration. These are test values, with no
provider or user credentials.

Rust tests compare encoding and decoding against those unchanged independent
vectors. Reproduce a separate file with:

```bash
cargo run --locked -p evm-state --bin evm-state-qualify -- \
  cursor-fixtures --output localdata/cursors-rust.json
cmp tests/fixtures/cursors.json localdata/cursors-rust.json
```

`proof-parity.json` retains independently generated account/storage proofs and
encoded header cases used to qualify the Rust proof and trie implementation.
The captured BSC lifecycle fixtures retain their provenance and proof bundles in
`lifecycle/manifest.json` and the adjacent evidence files.
