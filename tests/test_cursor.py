import base64
import json
from pathlib import Path

import pytest
from nacl.secret import SecretBox

from evm_state.cursor import PUBLIC_KEY, PUBLIC_NONCE, decode
from evm_state.proof import VerificationError

CURSORS = json.loads((Path(__file__).parent / "fixtures/cursors.json").read_text())
HASH = "a" * 64
HEAD = "b" * 64


def opaque(payload):
    return base64.urlsafe_b64encode(SecretBox(PUBLIC_KEY).encrypt(payload.encode(), PUBLIC_NONCE).ciphertext).decode()


@pytest.mark.parametrize("step", ["1", "17"])
def test_decode_vectors_generated_by_upstream_go_implementation(step):
    for number, token in CURSORS[step].items():
        value = decode(token)
        assert value["step"] == int(step)
        assert value["block"] == {"number": int(number), "hash": "0x" + f"{int(number):064x}"}
        assert value["head"] == value["lib"] == value["block"]


def test_finalized_cursor_can_have_a_later_head():
    value = decode(opaque(f"c2:17:100:{HASH}:103:{HEAD}"))
    assert value["block"] == value["lib"] == {"number": 100, "hash": "0x" + HASH}
    assert value["head"] == {"number": 103, "hash": "0x" + HEAD}


@pytest.mark.parametrize("payload", ["c1", f"c1:1:100:{HASH}:99:{HEAD}", f"c1:2:100:{HASH}:100:{HASH}",
    f"c2:17:100:{HASH}:99:{HEAD}", f"c2:17:100:{HASH}:100:{HEAD}", f"c2:17:-1:{HASH}:100:{HEAD}",
    f"c2:17:{2**64}:{HASH}:{2**64}:{HASH}", f"c3:17:100:{HASH}:103:{HEAD}:99:{HASH}",
    f"c1:17:100:bad:100:bad", f"c4:17:100:{HASH}:100:{HASH}"])
def test_invalid_or_nonfinalized_cursor_is_rejected(payload):
    with pytest.raises(VerificationError):
        decode(opaque(payload))


@pytest.mark.parametrize("token", ["", "abc", "!not-base64!", "a" * 2049, CURSORS["1"]["100"][1:],
                                    CURSORS["1"]["100"].replace("_", "/").replace("-", "+")])
def test_torn_or_corrupt_opaque_cursor_is_rejected(token):
    with pytest.raises(VerificationError):
        decode(token)
