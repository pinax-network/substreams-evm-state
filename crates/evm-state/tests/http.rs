use anyhow::Result;
use evm_state::ch::{params, ClickHouse};
use serde_json::json;
use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
    time::Duration,
};

fn fixture(response: String) -> Result<(ClickHouse, thread::JoinHandle<()>)> {
    let server = TcpListener::bind("127.0.0.1:0")?;
    let url = format!("http://{}", server.local_addr()?);
    let handle = thread::spawn(move || {
        let (mut stream, _) = server.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut input = Vec::new();
        let mut byte = [0];
        while !input.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            input.push(byte[0]);
        }
        let text = String::from_utf8(input).unwrap();
        let length = text
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length: ")
                    .and_then(|v| v.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        let mut body = vec![0; length];
        stream.read_exact(&mut body).unwrap();
        stream.write_all(response.as_bytes()).unwrap();
    });
    Ok((
        ClickHouse::configured("test", &url, "user", "dummy-secret")?,
        handle,
    ))
}
fn response(body: &str, headers: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n{body}",
        body.len()
    )
}

#[test]
fn http_success_status_cannot_hide_failed_insert_or_query() -> Result<()> {
    for (body, headers) in [
        ("Code: 241. DB::Exception: dummy-secret", ""),
        ("", "X-ClickHouse-Exception-Code: 241\r\n"),
    ] {
        let (client, server) = fixture(response(body, headers))?;
        let error = client
            .execute("INSERT INTO target SELECT 1", &Default::default())
            .unwrap_err();
        assert!(!format!("{error:#}").contains("dummy-secret"));
        server.join().unwrap();
        let (client, server) = fixture(response(body, headers))?;
        let error = client
            .insert_values("target", [json!({"value":1})])
            .unwrap_err();
        assert!(!format!("{error:#}").contains("dummy-secret"));
        server.join().unwrap();
    }
    Ok(())
}

#[test]
fn partial_json_stream_and_truncated_http_body_are_rejected() -> Result<()> {
    let (client, server) = fixture(response(
        "{\"n\":1}\nCode: 241. DB::Exception: secret\n",
        "",
    ))?;
    let mut rows = client.rows("SELECT n", &Default::default())?;
    assert_eq!(rows.next().unwrap()?, json!({"n":1}));
    let error = rows.next().unwrap().unwrap_err();
    assert!(!format!("{error:#}").contains("secret"));
    assert!(rows.next().is_none());
    server.join().unwrap();
    let (client, server) = fixture(
        "HTTP/1.1 200 OK\r\nContent-Length: 99\r\nConnection: close\r\n\r\n{\"n\":1}\n".into(),
    )?;
    assert!(client.one("SELECT n", &Default::default()).is_err());
    server.join().unwrap();
    Ok(())
}

#[test]
fn query_parameters_cannot_change_sql_identifiers_and_uint64_stays_exact() -> Result<()> {
    assert!(ClickHouse::new("bad; DROP DATABASE x").is_err());
    let (client, server) = fixture(response("{\"n\":\"18446744073709551615\"}\n", ""))?;
    let row = client.one(
        "SELECT n WHERE value={value:String}",
        &params(json!({"value":"x&query=DROP TABLE t"}))?,
    )?;
    assert_eq!(evm_state::ch::uint(&row["n"])?, u64::MAX);
    server.join().unwrap();
    for value in [
        json!(true),
        json!(-1),
        json!(1.2),
        json!("18446744073709551616"),
    ] {
        assert!(evm_state::ch::uint(&value).is_err());
    }
    Ok(())
}
