import pytest
import rlp

from evm_state.header import FIELDS, encode_rpc_header, verify_header
from evm_state.proof import VerificationError, keccak256


def header(optional=6):
    values, fields = {}, []
    for i, (name, length, bits) in enumerate(FIELDS[:15 + optional]):
        if bits:
            values[name] = hex(i)
            fields.append(i)
        else:
            value = bytes([i]) * (length or 8)
            values[name] = "0x" + value.hex()
            fields.append(value)
    payload = rlp.encode(fields)
    values["hash"] = "0x" + keccak256(payload).hex()
    summary = {"hash": values["hash"], "state_root": values["stateRoot"], "parent_hash": values["parentHash"],
               "number": 8, "timestamp": 11}
    return values, summary, "0x" + payload.hex()


@pytest.mark.parametrize("optional", [0, 1, 2, 5, 6, 8])
def test_header_hash_covers_legacy_and_optional_fork_fields(optional):
    values, summary, encoded = header(optional)
    assert encode_rpc_header(values) == encoded
    assert verify_header(encoded, summary, values["hash"]) == bytes.fromhex(values["hash"][2:])


@pytest.mark.parametrize("field", ["stateRoot", "number", "timestamp", "parentHash", "requestsHash"])
def test_provider_cannot_change_header_fields_while_retaining_pinned_hash(field):
    values, _, _ = header()
    values[field] = "0x55" if field in {"number", "timestamp"} else "0x" + "55" * 32
    with pytest.raises(VerificationError, match="block hash"):
        encode_rpc_header(values)


@pytest.mark.parametrize("field", ["state_root", "number", "timestamp", "parent_hash", "hash"])
def test_manifest_cannot_substitute_another_root_or_height(field):
    _, summary, encoded = header()
    summary[field] = 42 if field in {"number", "timestamp"} else "0x" + "55" * 32
    with pytest.raises(VerificationError):
        verify_header(encoded, summary)


def test_missing_required_header_field_fails_closed():
    values, _, _ = header()
    del values["stateRoot"]
    with pytest.raises(VerificationError, match="missing stateRoot"):
        encode_rpc_header(values)


def test_real_bsc_header_hash_matches_recorded_bootstrap_block():
    import json
    from pathlib import Path
    values = json.loads((Path(__file__).parent / "fixtures/bsc-121294292-header.json").read_text())
    encoded = encode_rpc_header(values)
    assert keccak256(bytes.fromhex(encoded[2:])).hex() == "d0987f468e3b66fa2593eb2df41924fed5fd563c697828916416de436784bd90"
