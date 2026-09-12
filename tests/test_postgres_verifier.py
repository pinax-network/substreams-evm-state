import copy

import pytest

from evm_state import postgres
from evm_state.proof import keccak256
from state_fixtures import A, CODE, proof_bundle, state, word


def fixture():
    bundle = proof_bundle(101, {A: state({1: 7})})
    captured = {"header": {k: bundle["header"][k] for k in ["number", "hash", "state_root"]},
        "accounts": [{"address": A, "nonce": "1", "balance": "25", "code": CODE,
            "code_hash": "0x" + keccak256(bytes.fromhex(CODE[2:])).hex(), "last_block": 100}],
        "storage": [{"address": A, "slot": word(1), "value": word(7), "last_block": 100},
                    {"address": A, "slot": word(2), "value": word(0), "last_block": 101}]}
    return captured, bundle


def test_complete_legacy_state_proves_account_and_all_nonzero_storage():
    captured, bundle = fixture()
    result = postgres.verify(captured, bundle, complete=True)
    assert result["storage_completeness_verified"]
    assert result["storage_slots_checked"] == 1
    assert not result["published_checkpoint"]


@pytest.mark.parametrize("field", ["nonce", "balance", "code_hash", "code"])
@pytest.mark.parametrize("missing", [True, False])
def test_legacy_metadata_missing_or_mismatch_cannot_pass_even_when_storage_root_matches(field, missing):
    captured, bundle = fixture()
    captured["accounts"][0][field] = None if missing else {
        "nonce": "2", "balance": "26", "code_hash": word(0), "code": "0x"}[field]
    with pytest.raises(ValueError):
        postgres.verify(captured, bundle, complete=True)


@pytest.mark.parametrize("defect", ["missing_slot", "extra_slot", "wrong_root", "wrong_hash", "proof", "future_row", "no_head", "no_account", "duplicate"])
def test_legacy_completeness_and_snapshot_corruption_are_rejected(defect):
    captured, bundle = fixture()
    if defect == "missing_slot": captured["storage"] = captured["storage"][1:]
    elif defect == "extra_slot": captured["storage"][1]["value"] = word(9)
    elif defect == "wrong_root": captured["header"]["state_root"] = word(0)
    elif defect == "wrong_hash": captured["header"]["hash"] = word(0)
    elif defect == "proof": bundle["accounts"][A]["proof"]["accountProof"] = []
    elif defect == "future_row": captured["accounts"][0]["last_block"] = 102
    elif defect == "no_head": captured["header"] = None
    elif defect == "no_account": captured["accounts"] = []
    else: captured["storage"].append(copy.deepcopy(captured["storage"][0]))
    with pytest.raises(ValueError):
        postgres.verify(captured, bundle, complete=True)


@pytest.mark.parametrize("requested", [0, 100, 102])
def test_block_option_never_labels_current_tables_as_historical_state(requested):
    captured, bundle = fixture()
    with pytest.raises(ValueError, match="historical state"):
        postgres.verify(captured, bundle, complete=True, requested_block=requested)


def test_sample_parity_is_explicitly_incomplete_and_checks_header_after_rpc():
    captured, bundle = fixture()
    class Provider:
        changed = False
        def call(self, method, args):
            assert args[-1] == hex(101) if method == "eth_getStorageAt" else args == [hex(101), False]
            if method == "eth_getStorageAt":
                return word(7 if args[1] == word(1) else 0)
            return {"hash": word(999 if self.changed else 101)}
    rpc = Provider()
    result = postgres.verify(captured, bundle, rpc=rpc)
    assert result["status"] == "sample-parity-only"
    assert result["storage_slots_checked"] == 2
    assert not result["storage_completeness_verified"]
    rpc.changed = True
    with pytest.raises(ValueError, match="header changed"):
        postgres.verify(captured, bundle, rpc=rpc)


def test_cli_exits_nonzero_for_metadata_mismatch(monkeypatch, capsys):
    captured, bundle = fixture()
    captured["accounts"][0]["nonce"] = "2"
    monkeypatch.setattr(postgres, "snapshot", lambda *args: captured)
    monkeypatch.setattr(postgres.RPC, "capture", lambda *args: bundle)
    with pytest.raises(SystemExit) as error:
        postgres.main(complete=True, argv=[A])
    assert error.value.code == 1
    assert "nonce/balance mismatch" in capsys.readouterr().err


@pytest.mark.parametrize("selected,limit", [("0x' OR TRUE--", 1000), (A, -1), (A, True)])
def test_untrusted_sql_inputs_are_rejected_before_running_psql(monkeypatch, selected, limit):
    def unexpected(*args, **kwargs):
        pytest.fail("invalid inputs reached psql")
    monkeypatch.setattr(postgres.subprocess, "run", unexpected)
    with pytest.raises(ValueError):
        postgres.snapshot(selected, limit)


def test_query_timeout_does_not_expose_database_credentials(monkeypatch):
    def timeout(command, **kwargs):
        raise postgres.subprocess.TimeoutExpired(command, 120)
    monkeypatch.setattr(postgres.subprocess, "run", timeout)
    with pytest.raises(RuntimeError) as error:
        postgres.snapshot(A, dsn="postgresql://user:private-dummy-password@example.invalid/db")
    assert str(error.value) == "PostgreSQL snapshot query timed out"
