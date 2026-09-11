import copy

import pytest

from evm_state.checkpoint import build, manifest, read_account
from evm_state.proof import VerificationError
from state_fixtures import A, B, CODE, block, insert_blocks, proof_bundle, source, state, word

pytestmark = pytest.mark.clickhouse


def storage(client, snapshot_id, account=A):
    return {row["slot"]: row["value"] for row in client.rows(
        "SELECT slot,value FROM checkpoint_storage FINAL WHERE snapshot_id={id:String} AND address={address:String}",
        {"id": snapshot_id, "address": account})}


def initial(databases):
    target, stream = databases(False), databases()
    bundle = proof_bundle(101, {A: state({1: 7, 2: 8})})
    insert_blocks(stream, [block(100, bundle, storage={(A, 1): 7, (A, 2): 8}, nonces={A: 1}, codes={A: CODE}),
                           block(101, bundle)])
    ready = build(target, bundle, [source(stream, 100)])
    return target, ready


def test_onboarding_at_common_block_preserves_old_checkpoint_and_independent_fields(databases):
    target, base = initial(databases)
    live, new = databases(), databases()
    bundle = proof_bundle(103, {A: state({2: 9}, balance=50), B: state({7: 11})})
    insert_blocks(live, [block(102, bundle, [A], storage={(A, 1): 0, (A, 2): 9}, balances={A: 50}), block(103, bundle, [A])])
    insert_blocks(new, [block(100, bundle, [B], storage={(B, 7): 11}), *[block(n, bundle, [B]) for n in range(101, 104)]])
    args = (target, bundle, [source(live, 102), source(new, 100, [B])], base["snapshot_id"])
    ready = build(*args)
    assert storage(target, ready["snapshot_id"]) == {word(2): word(9)}
    assert storage(target, ready["snapshot_id"], B) == {word(7): word(11)}
    assert storage(target, base["snapshot_id"]) == {word(1): word(7), word(2): word(8)}
    current = read_account(target, ready["snapshot_id"], A)
    assert (current["nonce"], current["balance"], current["code"]) == (1, "50", CODE)
    assert read_account(target, base["snapshot_id"], A)["balance"] == "25"
    with pytest.raises(VerificationError, match="not ready"):
        read_account(target, base["snapshot_id"], B)
    replay = build(*args)
    assert replay["snapshot_id"] != ready["snapshot_id"]
    assert replay["state_sha256"] == ready["state_sha256"]


@pytest.mark.parametrize("defect", ["gap", "fork", "parent", "filter", "schema", "proof", "wrong_nonce", "wrong_code", "missing_slot", "extra_account"])
def test_bad_candidate_never_replaces_ready_state(databases, defect):
    target, base = initial(databases)
    stream = databases()
    bundle = proof_bundle(103, {A: state({1: 7, 2: 9})})
    rows = [block(102, bundle, storage={(A, 2): 9}), block(103, bundle)]
    if defect == "gap": rows.pop(0)
    elif defect == "fork":
        other = copy.deepcopy(rows[0]); other["hash"] = word(555); rows.append(other)
    elif defect == "parent": rows[0]["parent_hash"] = word(555)
    elif defect == "filter": rows[0]["accounts"] = B
    elif defect == "schema": rows[0]["schema_version"] = 2
    elif defect == "proof": bundle["accounts"][A]["proof"]["accountProof"] = []
    elif defect == "wrong_nonce": rows[0]["nonces"] = [{"address": A, "value": 99, "ordinal": 1}]
    elif defect == "wrong_code": rows[0]["codes"] = block(102, bundle, codes={A: "0x"})["codes"]
    elif defect == "missing_slot": rows[0]["storage"] = []
    elif defect == "extra_account": rows[0]["storage"] += block(102, bundle, storage={(B, 1): 7})["storage"]
    insert_blocks(stream, rows)
    with pytest.raises(VerificationError):
        build(target, bundle, [source(stream, 102)], base["snapshot_id"])
    assert list(target.rows("SELECT snapshot_id FROM ready_checkpoints")) == [{"snapshot_id": base["snapshot_id"]}]
    assert manifest(target, base["snapshot_id"])["state_sha256"] == base["state_sha256"]


@pytest.mark.parametrize("table", ["checkpoint_accounts", "checkpoints"])
def test_interrupted_publication_does_not_expose_partial_candidate(databases, monkeypatch, table):
    target, base = initial(databases)
    stream = databases()
    bundle = proof_bundle(102, {A: state({1: 7, 2: 8})})
    insert_blocks(stream, [block(102, bundle)])
    original = target.insert

    def interrupted(name, rows, **kwargs):
        if name == table:
            # The old checkpoint remains readable after storage has been inserted,
            # and again after account rows have been inserted but before publication.
            assert read_account(target, base["snapshot_id"], A)["balance"] == "25"
            assert int(target.one("SELECT countDistinct(snapshot_id) AS n FROM checkpoint_storage")["n"]) == 2
            candidate = target.one("SELECT any(snapshot_id) AS id FROM checkpoint_storage WHERE snapshot_id != {base:String}",
                                   {"base": base["snapshot_id"]})["id"]
            with pytest.raises(ValueError):
                read_account(target, candidate, A)
            raise OSError("injected publisher interruption")
        return original(name, rows, **kwargs)

    monkeypatch.setattr(target, "insert", interrupted)
    with pytest.raises(OSError, match="injected"):
        build(target, bundle, [source(stream, 102)], base["snapshot_id"])
    assert target.one("SELECT snapshot_id FROM ready_checkpoints")["snapshot_id"] == base["snapshot_id"]
    monkeypatch.setattr(target, "insert", original)
    resumed = build(target, bundle, [source(stream, 102)], base["snapshot_id"])
    assert storage(target, resumed["snapshot_id"]) == storage(target, base["snapshot_id"])


def test_proven_empty_code_and_zero_storage_are_published(databases):
    target, base = initial(databases)
    stream = databases()
    bundle = proof_bundle(102, {A: state({}, nonce=0, balance=0, code="0x", exists=False)})
    insert_blocks(stream, [block(102, bundle, storage={(A, 1): 0, (A, 2): 0}, nonces={A: 0}, balances={A: 0}, codes={A: "0x"})])
    ready = build(target, bundle, [source(stream, 102)], base["snapshot_id"])
    assert storage(target, ready["snapshot_id"]) == {}
    account = read_account(target, ready["snapshot_id"], A)
    assert (account["exists"], account["code"], account["nonce"], account["balance"]) == (False, "0x", 0, "0")


def test_disk_budget_rejects_before_allocating_candidate(databases):
    target, base = initial(databases)
    stream = databases()
    bundle = proof_bundle(102, {A: state({1: 7, 2: 8})})
    insert_blocks(stream, [block(102, bundle)])
    with pytest.raises(VerificationError, match="budget"):
        build(target, bundle, [source(stream, 102)], base["snapshot_id"], budget_bytes=1)
    assert int(target.one("SELECT countDistinct(snapshot_id) AS n FROM checkpoint_storage")["n"]) == 1


def test_pinned_hash_claim_requires_header_commitment(databases):
    target, stream = databases(False), databases()
    bundle = proof_bundle(100, {A: state()})
    bundle["header_trust"] = "operator-pinned-hash"
    insert_blocks(stream, [block(100, bundle)])
    with pytest.raises(VerificationError, match="encoded header"):
        build(target, bundle, [source(stream, 100)])
