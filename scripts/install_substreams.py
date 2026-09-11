#!/usr/bin/env python3
"""Install the qualification CLI from a checksummed upstream release archive."""
import argparse
import hashlib
from pathlib import Path
import platform
import tarfile
import tempfile
import urllib.request

VERSION = "1.22.0"
CHECKSUMS = {
    "darwin_arm64": "80ec00a9a89d18402420f8ae9f79575ab4696107c38169e2e81408d51e346d17",
    "darwin_x86_64": "2b91ff37978c7f0179ade469d0f4cad5cc2454580ed480255793f27bbad2fdcf",
    "linux_arm64": "385ac239bf792e09936f19e08ac419c4bab64d79415c06a423f29ed8fd4c278b",
    "linux_x86_64": "6eef6182d8d9e3c0147a3d03f38fc2b9c0f96900d040132294a0b540e691ae67",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, default=Path("localdata/toolchain/bin"))
    args = parser.parse_args()
    machine = {"aarch64": "arm64", "amd64": "x86_64"}.get(platform.machine().lower(), platform.machine().lower())
    target = platform.system().lower() + "_" + machine
    if target not in CHECKSUMS:
        parser.error("supported platforms are Linux/WSL or macOS on ARM64 or x86-64")
    name = "substreams_" + target + ".tar.gz"
    url = f"https://github.com/streamingfast/substreams/releases/download/v{VERSION}/{name}"
    with tempfile.TemporaryDirectory(prefix="substreams-install-") as directory:
        archive = Path(directory) / name
        with urllib.request.urlopen(url, timeout=120) as response, archive.open("wb") as output:
            while data := response.read(1024 * 1024):
                output.write(data)
        if hashlib.sha256(archive.read_bytes()).hexdigest() != CHECKSUMS[target]:
            raise RuntimeError("Substreams archive checksum mismatch")
        with tarfile.open(archive) as tar:
            members = [member for member in tar.getmembers() if member.isfile() and member.name == "substreams"]
            if len(members) != 1:
                raise RuntimeError("unexpected release archive layout")
            binary = tar.extractfile(members[0]).read()
        args.destination.mkdir(parents=True, exist_ok=True)
        path = args.destination / "substreams"
        path.write_bytes(binary)
        path.chmod(0o755)
        print(f"Installed Substreams v{VERSION}: {path.resolve()}")


if __name__ == "__main__":
    main()
