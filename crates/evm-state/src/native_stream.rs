//! Loopback-only deterministic gRPC output for qualification of the native sink.
//! This serves packaged protobuf schemas; mapper execution is tested separately.
use crate::{cursor, proof::string};
use anyhow::{ensure, Context, Result};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full, StreamBody};
use hyper::{
    body::{Bytes, Frame, Incoming},
    service::service_fn,
    Request, Response,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, Value as ProtoValue};
use serde_json::{json, Value};
use std::{
    convert::Infallible,
    fs,
    io::Read,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

const LIMIT: usize = 64 * 1024 * 1024;
type Body = UnsyncBoxBody<Bytes, Infallible>;
type Sender = mpsc::Sender<std::result::Result<Frame<Bytes>, Infallible>>;

pub fn decode_frame(frame: &[u8], encoding: &str) -> Result<Vec<u8>> {
    ensure!(frame.len() >= 5, "truncated gRPC frame");
    let length = u32::from_be_bytes(frame[1..5].try_into().unwrap()) as usize;
    ensure!(
        length <= LIMIT && frame.len() == length + 5,
        "gRPC request must contain one bounded frame"
    );
    let payload = &frame[5..];
    match frame[0] {
        0 => Ok(payload.to_vec()),
        1 => {
            ensure!(encoding == "s2", "unsupported gRPC request compression");
            let mut decoded = Vec::new();
            minlz::s2::Reader::new(payload)
                .take(LIMIT as u64 + 1)
                .read_to_end(&mut decoded)?;
            ensure!(
                decoded.len() <= LIMIT,
                "decoded gRPC request exceeds size limit"
            );
            Ok(decoded)
        }
        _ => anyhow::bail!("invalid gRPC compression flag"),
    }
}
fn frame(message: DynamicMessage) -> Result<Frame<Bytes>> {
    let bytes = message.encode_to_vec();
    ensure!(bytes.len() <= LIMIT, "gRPC output exceeds size limit");
    let mut framed = Vec::with_capacity(bytes.len() + 5);
    framed.push(0);
    framed.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    framed.extend(bytes);
    Ok(Frame::data(framed.into()))
}
fn assign(message: &mut DynamicMessage, path: &[&str], value: ProtoValue) -> Result<()> {
    if path.len() == 1 {
        message
            .try_set_field_by_name(path[0], value)
            .context("invalid packaged protobuf field")?;
        return Ok(());
    }
    let child = message
        .get_field_by_name_mut(path[0])
        .context("missing packaged protobuf field")?;
    let ProtoValue::Message(child) = child else {
        anyhow::bail!("packaged protobuf field is not a message")
    };
    assign(child, &path[1..], value)
}

pub type BeforeBlock = Arc<dyn Fn(u64, &StreamContext) -> Result<()> + Send + Sync>;
pub type AfterBlocks = Arc<dyn Fn(&StreamContext) -> Result<()> + Send + Sync>;
#[derive(Default)]
pub struct StreamOptions {
    pub backfill: bool,
    pub before_block: Option<BeforeBlock>,
    pub after_blocks: Option<AfterBlocks>,
}
#[derive(Clone)]
pub struct StreamContext {
    closed: Arc<AtomicBool>,
    sender: Sender,
}
impl StreamContext {
    pub fn active(&self) -> bool {
        !self.closed.load(Ordering::Relaxed) && !self.sender.is_closed()
    }
    pub fn wait_until(
        &self,
        mut predicate: impl FnMut() -> Result<bool>,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        while !predicate()? {
            ensure!(self.active(), "native request closed during fixture wait");
            ensure!(
                Instant::now() < deadline,
                "timed out waiting for native sink evidence"
            );
            thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    }
    pub fn hold(&self) {
        while self.active() {
            thread::sleep(Duration::from_millis(20));
        }
    }
}
struct Block {
    number: u64,
    hash: String,
    timestamp: u64,
    data: Bytes,
}
struct State {
    request: MessageDescriptor,
    response: MessageDescriptor,
    blocks: Vec<Block>,
    options: StreamOptions,
    closed: Arc<AtomicBool>,
    requests: Mutex<Vec<Value>>,
    errors: Mutex<Vec<String>>,
}

pub struct NativeStream {
    pub endpoint: String,
    state: Arc<State>,
    stop: Option<oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}
impl NativeStream {
    pub fn new(package: &Path, blocks: &[Value], options: StreamOptions) -> Result<Self> {
        // Package field 1 is a repeated FileDescriptorProto, wire compatible
        // with FileDescriptorSet. Unknown package fields are ignored by prost.
        let descriptors = prost_types::FileDescriptorSet::decode(fs::read(package)?.as_slice())?;
        let pool = DescriptorPool::from_file_descriptor_set(descriptors)?;
        let block_type = pool
            .get_message_by_name("evm.state.v1.BlockState")
            .context("package has no BlockState schema")?;
        let mut encoded = Vec::new();
        for row in blocks {
            let wire = wire_block(row)?;
            let bytes = DynamicMessage::deserialize(block_type.clone(), &wire)?.encode_to_vec();
            let hash = string(row, "hash")?;
            crate::proof::fixed::<32>(hash)?;
            encoded.push(Block {
                number: crate::ch::uint(&row["number"])?,
                hash: hash.trim_start_matches("0x").into(),
                timestamp: crate::ch::uint(&row["timestamp"])?,
                data: bytes.into(),
            });
        }
        let closed = Arc::new(AtomicBool::new(false));
        let state = Arc::new(State {
            request: pool
                .get_message_by_name("sf.substreams.rpc.v2.Request")
                .context("package has no RPC Request schema")?,
            response: pool
                .get_message_by_name("sf.substreams.rpc.v2.Response")
                .context("package has no RPC Response schema")?,
            blocks: encoded,
            options,
            closed,
            requests: Mutex::new(Vec::new()),
            errors: Mutex::new(Vec::new()),
        });
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let (stop, mut stopped) = oneshot::channel();
        let serving = state.clone();
        let thread = thread::spawn(move || {
            runtime.block_on(async move {
                let listener=match tokio::net::TcpListener::from_std(listener) {Ok(listener)=>listener,Err(error)=>{serving.errors.lock().unwrap().push(error.to_string());return;}};
                loop {
                    tokio::select! {
                        _=&mut stopped=>break,
                        accepted=listener.accept()=>{
                            match accepted {
                                Ok((socket,_))=>{let state=serving.clone();tokio::spawn(async move {
                                    let service=service_fn(move|request|serve(request,state.clone()));
                                    // Client cancellation is expected in recovery tests.
                                    let _=hyper::server::conn::http2::Builder::new(TokioExecutor::new()).serve_connection(TokioIo::new(socket),service).await;
                                });},
                                Err(error)=>{serving.errors.lock().unwrap().push(error.to_string());break;}
                            }
                        }
                    }
                }
            });
            runtime.shutdown_timeout(Duration::from_secs(5));
        });
        Ok(Self {
            endpoint,
            state,
            stop: Some(stop),
            thread: Some(thread),
        })
    }
    pub fn requests(&self) -> Vec<Value> {
        self.state.requests.lock().unwrap().clone()
    }
    pub fn errors(&self) -> Vec<String> {
        self.state.errors.lock().unwrap().clone()
    }
}
impl Drop for NativeStream {
    fn drop(&mut self) {
        self.state.closed.store(true, Ordering::Relaxed);
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Accept both protobuf JSON fixture rows and the equivalent flattened SQL row.
pub fn wire_block(row: &Value) -> Result<Value> {
    let mut wire = row
        .as_object()
        .context("invalid block fixture")?
        .iter()
        .filter(|(k, _)| !k.starts_with('_') && !k.contains('.'))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<serde_json::Map<_, _>>();
    for (group, fields) in [
        ("storage", vec!["address", "slot", "value", "ordinal"]),
        ("balances", vec!["address", "value", "ordinal"]),
        ("nonces", vec!["address", "value", "ordinal"]),
        ("codes", vec!["address", "hash", "code", "ordinal"]),
        ("lifecycle", vec!["address", "kind", "ordinal"]),
    ] {
        let Some(addresses) = row.get(format!("{group}.address")) else {
            continue;
        };
        let count = addresses.as_array().context("invalid fixture array")?.len();
        let mut values = vec![json!({}); count];
        for field in fields {
            let array = row[format!("{group}.{field}")]
                .as_array()
                .context("missing fixture array")?;
            ensure!(
                array.len() == count,
                "fixture nested arrays have different lengths"
            );
            for (value, entry) in values.iter_mut().zip(array) {
                value[field] = entry.clone();
            }
        }
        wire.insert(group.into(), json!(values));
    }
    Ok(Value::Object(wire))
}

fn grpc_error(code: &str) -> Response<Body> {
    Response::builder()
        .header("content-type", "application/grpc")
        .header("grpc-status", code)
        .body(Full::new(Bytes::new()).boxed_unsync())
        .unwrap()
}
async fn serve(
    request: Request<Incoming>,
    state: Arc<State>,
) -> std::result::Result<Response<Body>, Infallible> {
    if request.uri().path() != "/sf.substreams.rpc.v2.Stream/Blocks" {
        return Ok(grpc_error("12"));
    }
    let encoding = request
        .headers()
        .get("grpc-encoding")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("identity")
        .to_owned();
    // Never retain authorization or API-key headers from the native client.
    let workers = request
        .headers()
        .get("x-substreams-parallel-workers")
        .and_then(|h| h.to_str().ok())
        .map(str::to_owned);
    let received = http_body_util::Limited::new(request.into_body(), LIMIT + 5)
        .collect()
        .await;
    let decoded = (|| -> Result<_> {
        let body = received
            .map_err(|_| anyhow::anyhow!("invalid or oversized gRPC body"))?
            .to_bytes();
        let message = DynamicMessage::decode(
            state.request.clone(),
            decode_frame(&body, &encoding)?.as_slice(),
        )
        .context("invalid packaged gRPC request")?;
        Ok((message, body.first() == Some(&1)))
    })();
    let (request, compressed_frame) = match decoded {
        Ok(request) => request,
        Err(error) => {
            state.errors.lock().unwrap().push(error.to_string());
            return Ok(grpc_error("13"));
        }
    };
    let (sender, receiver) = mpsc::channel(4);
    let ctx = StreamContext {
        closed: state.closed.clone(),
        sender,
    };
    tokio::task::spawn_blocking(move || {
        let result = stream(&state, &request, workers, &encoding, compressed_frame, &ctx);
        if let Err(error) = &result {
            if ctx.active() {
                state.errors.lock().unwrap().push(error.to_string());
            }
        }
        let mut trailers = hyper::HeaderMap::new();
        trailers.insert(
            "grpc-status",
            if result.is_ok() { "0" } else { "13" }.parse().unwrap(),
        );
        let _ = ctx.sender.blocking_send(Ok(Frame::trailers(trailers)));
    });
    Ok(Response::builder()
        .header("content-type", "application/grpc")
        .body(StreamBody::new(ReceiverStream::new(receiver)).boxed_unsync())
        .unwrap())
}
fn stream(
    state: &State,
    request: &DynamicMessage,
    workers: Option<String>,
    encoding: &str,
    compressed_frame: bool,
    context: &StreamContext,
) -> Result<()> {
    let get = |name: &str| {
        request
            .get_field_by_name(name)
            .map(|v| v.into_owned())
            .context("missing packaged request field")
    };
    ensure!(
        get("final_blocks_only")?.as_bool() == Some(true),
        "fixture requires finalized blocks"
    );
    let raw_start = get("start_block_num")?
        .as_i64()
        .context("invalid request start")?;
    let mut start = u64::try_from(raw_start).context("fixture requires an absolute start block")?;
    let stop = get("stop_block_num")?
        .as_u64()
        .context("invalid request stop")?;
    let cursor = get("start_cursor")?;
    let cursor = cursor.as_str().context("invalid request cursor")?;
    if !cursor.is_empty() {
        start = cursor::decode(cursor)?
            .block
            .number
            .checked_add(1)
            .context("cursor overflow")?;
    }
    state.requests.lock().unwrap().push(json!({"start_block":start,"start_cursor":cursor,"stop_block":stop,"workers":workers,"compression":encoding,"compressed_frame":compressed_frame}));
    let mut session = DynamicMessage::new(state.response.clone());
    assign(
        &mut session,
        &["session", "trace_id"],
        ProtoValue::String("local-rust-native-sink-integration".into()),
    )?;
    assign(
        &mut session,
        &["session", "resolved_start_block"],
        ProtoValue::U64(start),
    )?;
    assign(
        &mut session,
        &["session", "linear_handoff_block"],
        ProtoValue::U64(if state.options.backfill {
            start.max(1000)
        } else {
            start
        }),
    )?;
    context.sender.blocking_send(Ok(frame(session)?))?;
    for block in &state.blocks {
        if block.number < start || stop != 0 && block.number >= stop {
            continue;
        }
        if let Some(before) = &state.options.before_block {
            before(block.number, context)?;
        }
        if !context.active() {
            return Ok(());
        }
        let mut response = DynamicMessage::new(state.response.clone());
        assign(
            &mut response,
            &["block_scoped_data", "output", "name"],
            ProtoValue::String("map_block_state".into()),
        )?;
        assign(
            &mut response,
            &["block_scoped_data", "output", "map_output", "type_url"],
            ProtoValue::String("type.googleapis.com/evm.state.v1.BlockState".into()),
        )?;
        assign(
            &mut response,
            &["block_scoped_data", "output", "map_output", "value"],
            ProtoValue::Bytes(block.data.clone()),
        )?;
        assign(
            &mut response,
            &["block_scoped_data", "clock", "id"],
            ProtoValue::String(block.hash.clone()),
        )?;
        assign(
            &mut response,
            &["block_scoped_data", "clock", "number"],
            ProtoValue::U64(block.number),
        )?;
        assign(
            &mut response,
            &["block_scoped_data", "clock", "timestamp", "seconds"],
            ProtoValue::I64(block.timestamp.try_into()?),
        )?;
        let cursor = cursor::encode_public(&format!(
            "c1:{}:{}:{}:{}:{}",
            if state.options.backfill { 17 } else { 1 },
            block.number,
            block.hash,
            block.number,
            block.hash
        ))?;
        assign(
            &mut response,
            &["block_scoped_data", "cursor"],
            ProtoValue::String(cursor),
        )?;
        assign(
            &mut response,
            &["block_scoped_data", "final_block_height"],
            ProtoValue::U64(block.number),
        )?;
        context.sender.blocking_send(Ok(frame(response)?))?;
    }
    if let Some(after) = &state.options.after_blocks {
        after(context)?;
    }
    Ok(())
}
