"""Serve deterministic protobuf output to the real native SQL sink, without RPC keys.

This exercises sink persistence, not WASM execution. Rust projection tests and the
separately recorded BSC replay cover the mapper and producer path.
"""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import subprocess
import threading
import time

import grpc
from google.protobuf import descriptor_pb2, descriptor_pool, json_format, message_factory

from conftest import SPKG, native_env, native_setup

CURSORS = json.loads((Path(__file__).parent / "fixtures/cursors.json").read_text())


def messages(package=SPKG):
    # Package.proto field 1 is repeated FileDescriptorProto, also the complete
    # FileDescriptorSet wire format. Use the exact packaged RPC/output schemas.
    descriptors = list(descriptor_pb2.FileDescriptorSet.FromString(package.read_bytes()).file)
    pool = descriptor_pool.DescriptorPool()
    while descriptors:
        pending = []
        for descriptor in descriptors:
            try:
                pool.Add(descriptor)
            except TypeError:
                pending.append(descriptor)
        if len(pending) == len(descriptors):
            raise AssertionError("unresolved packaged protobuf dependencies")
        descriptors = pending
    return {name: message_factory.GetMessageClass(pool.FindMessageTypeByName(full)) for name, full in {
        "Request": "sf.substreams.rpc.v2.Request", "Response": "sf.substreams.rpc.v2.Response",
        "BlockState": "evm.state.v1.BlockState"}.items()}


class NativeStream:
    def __init__(self, blocks, proxy_binary, before_block=None, after_blocks=None, backfill=False):
        self.types = messages()
        self.blocks = blocks
        self.before_block = before_block
        self.after_blocks = after_blocks
        self.backfill = backfill
        self.requests = []
        self.requested_workers = []
        self.errors = []
        self.closed = threading.Event()
        self.server = grpc.server(ThreadPoolExecutor(max_workers=2))
        self.server.add_generic_rpc_handlers([grpc.method_handlers_generic_handler(
            "sf.substreams.rpc.v2.Stream", {"Blocks": grpc.unary_stream_rpc_method_handler(
                self.stream, request_deserializer=self.types["Request"].FromString,
                response_serializer=lambda value: value.SerializeToString())})])
        port = self.server.add_insecure_port("127.0.0.1:0")
        self.server.start()
        self.proxy = subprocess.Popen([str(proxy_binary), "-target", f"127.0.0.1:{port}"],
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.endpoint = "http://" + self.proxy.stdout.readline().strip()

    def stream(self, request, context):
        self.requests.append(request)
        self.requested_workers.append(dict(context.invocation_metadata()).get("x-substreams-parallel-workers"))
        try:
            assert request.final_blocks_only
            start = request.start_block_num
            if request.start_cursor:
                matches = [int(number) for cursors in CURSORS.values() for number, value in cursors.items()
                           if value == request.start_cursor]
                assert len(matches) == 1, "unrecognized resume cursor"
                start = matches[0] + 1
            response = self.types["Response"]()
            response.session.trace_id = "local-native-sink-integration"
            response.session.resolved_start_block = start
            response.session.linear_handoff_block = 1000 if self.backfill else start
            yield response
            for data in self.blocks:
                number = data["number"]
                if number < start or (request.stop_block_num and number >= request.stop_block_num):
                    continue
                if self.before_block:
                    self.before_block(number, self, context)
                if not context.is_active():
                    return
                output = json_format.ParseDict(data, self.types["BlockState"]())
                response = self.types["Response"]()
                block = response.block_scoped_data
                block.output.name = "map_block_state"
                block.output.map_output.type_url = "type.googleapis.com/evm.state.v1.BlockState"
                block.output.map_output.value = output.SerializeToString()
                block.clock.id = data["hash"][2:]
                block.clock.number = number
                block.clock.timestamp.seconds = data["timestamp"]
                block.cursor = CURSORS["17" if self.backfill else "1"][str(number)]
                block.final_block_height = number
                yield response
            if self.after_blocks:
                self.after_blocks(self, context)
        except Exception as error:
            self.errors.append(error)
            context.abort(grpc.StatusCode.INTERNAL, str(error))

    def close(self):
        self.closed.set()
        self.server.stop(0).wait(timeout=5)
        self.proxy.terminate()
        self.proxy.communicate(timeout=5)


def wait_until(predicate, process=None, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        if process is not None and process.poll() is not None:
            raise AssertionError(f"native sink stopped unexpectedly ({process.returncode})")
        time.sleep(0.02)
    raise AssertionError("timed out waiting for native sink evidence")


class NativeRun:
    def __init__(self, client, directory, accounts, initialize=True):
        self.client, self.directory, self.accounts = client, directory, accounts
        if initialize:
            native_setup(client.database, directory)
        self.cursor = directory / "cursor.txt"
        self.process = None
        self.log = None

    def start(self, stream, stop=104):
        log_path = self.directory / f"run-{time.time_ns()}.log"
        self.log = log_path.open("w")
        self.process = subprocess.Popen(["substreams", "sink", "clickhouse", str(SPKG), "map_block_state",
            "-p", "map_block_state=" + ",".join(self.accounts), "-e", stream.endpoint,
            "-s", "100", "-t", str(stop), "--final-blocks-only", "--force-protocol-version", "2",
            "--sink-info-folder", str(self.directory / "meta"), "--cursor-file-path", str(self.cursor),
            "--spool-dir", str(self.directory / "spool"), "--spool-max-size", "16MiB",
            "--decode-batch-size", "1", "--spool-max-idle", "500ms", "--bytes-encoding", "0xhex",
            "--max-retries", "0", "--prometheus-addr", "127.0.0.1:0"],
            env=native_env(self.client.database), stdout=self.log, stderr=subprocess.STDOUT)
        return self.process

    def finish(self, expected=0):
        try:
            result = self.process.wait(timeout=30)
            self.log.close()
            output = Path(self.log.name).read_text()
            assert result == expected, output[-8000:]
            return output
        finally:
            self.close()

    def close(self):
        if self.process and self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
        if self.log:
            self.log.close()
