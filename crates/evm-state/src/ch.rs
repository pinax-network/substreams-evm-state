//! Streaming ClickHouse HTTP access. The upstream native sink owns block ingestion.
use anyhow::{bail, ensure, Context, Result};
use reqwest::blocking::{Client, Response};
use serde_json::{Map, Value};
use std::{
    env,
    io::{BufRead, BufReader, Lines, Read},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

pub type Params = Map<String, Value>;
pub type AdditionalCapacityGuard = Arc<dyn Fn(&str) -> Result<()> + Send + Sync>;

pub fn params(value: Value) -> Result<Params> {
    value
        .as_object()
        .cloned()
        .context("query parameters must be an object")
}

pub fn identifier(name: &str) -> Result<&str> {
    ensure!(
        !name.is_empty()
            && name.bytes().enumerate().all(|(i, v)| v == b'_'
                || v.is_ascii_alphabetic()
                || (i > 0 && v.is_ascii_digit())),
        "invalid ClickHouse database/table identifier"
    );
    Ok(name)
}

/// ClickHouse represents UInt64 values as decimal JSON strings by default.
pub fn uint(value: &Value) -> Result<u64> {
    if let Some(n) = value.as_u64() {
        return Ok(n);
    }
    let text = value.as_str().context("expected unsigned integer")?;
    ensure!(
        !text.is_empty() && text.bytes().all(|c| c.is_ascii_digit()),
        "expected unsigned integer"
    );
    text.parse().context("integer exceeds uint64")
}

#[derive(Clone)]
pub struct ClickHouse {
    pub database: String,
    pub url: String,
    pub control_home: PathBuf,
    user: String,
    password: String,
    http: Client,
    pub(crate) additional_capacity_guard: Option<AdditionalCapacityGuard>,
}

impl ClickHouse {
    pub fn new(database: &str) -> Result<Self> {
        Self::configured(
            database,
            &env::var("CH_HTTP_URL").unwrap_or_else(|_| "http://127.0.0.1:18123".into()),
            &env::var("CH_USER").unwrap_or_else(|_| "evm_state".into()),
            &env::var("CH_PASSWORD").unwrap_or_else(|_| "local-development-only".into()),
        )
    }

    pub fn configured(database: &str, url: &str, user: &str, password: &str) -> Result<Self> {
        identifier(database)?;
        let parsed =
            reqwest::Url::parse(url).map_err(|_| anyhow::anyhow!("invalid ClickHouse URL"))?;
        ensure!(
            matches!(parsed.scheme(), "http" | "https") && parsed.host_str().is_some(),
            "invalid ClickHouse URL"
        );
        Ok(Self {
            database: database.into(),
            url: url.into(),
            user: user.into(),
            password: password.into(),
            additional_capacity_guard: None,
            control_home: env::var_os("EVM_STATE_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| "localdata/control".into()),
            http: Client::builder()
                .timeout(Duration::from_secs(300))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }

    pub fn with_database(&self, database: &str) -> Result<Self> {
        identifier(database)?;
        let mut client = self.clone();
        client.database = database.into();
        Ok(client)
    }

    pub fn with_control_home(mut self, home: impl Into<PathBuf>) -> Self {
        self.control_home = home.into();
        self
    }

    /// Add a scoped admission restriction for embedded controllers and fault
    /// qualification. Success still runs the configured capacity policy; this
    /// cannot override or disable it. Cloned clients retain the restriction.
    pub fn with_additional_capacity_guard(mut self, guard: AdditionalCapacityGuard) -> Self {
        self.additional_capacity_guard = Some(guard);
        self
    }

    pub fn request(&self, sql: &str, params: &Params, body: Vec<u8>) -> Result<Response> {
        let mut query = vec![
            ("database".to_owned(), self.database.clone()),
            ("query".to_owned(), sql.to_owned()),
        ];
        for (key, value) in params {
            let text = match value {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(v) => if *v { "True" } else { "False" }.into(),
                _ => bail!("invalid query parameter value"),
            };
            query.push((format!("param_{key}"), text));
        }
        let response = self
            .http
            .post(format!("{}/", self.url.trim_end_matches('/')))
            .query(&query)
            .basic_auth(&self.user, Some(&self.password))
            .body(body)
            .send()
            .map_err(|e| {
                anyhow::anyhow!(
                    "ClickHouse request failed: {}",
                    if e.is_timeout() {
                        "timeout"
                    } else {
                        "transport error"
                    }
                )
            })?;
        // Error bodies can contain SQL, credentials in external-table URLs, or cursors.
        ensure!(
            response.status().is_success(),
            "ClickHouse query failed (HTTP {})",
            response.status().as_u16()
        );
        if let Some(code) = response.headers().get("X-ClickHouse-Exception-Code") {
            ensure!(
                code.to_str().ok() == Some("0"),
                "ClickHouse reported a query exception"
            );
        }
        Ok(response)
    }

    pub fn execute(&self, sql: &str, params: &Params) -> Result<String> {
        let mut result = String::new();
        self.request(sql, params, Vec::new())?
            .read_to_string(&mut result)
            .context("incomplete ClickHouse response")?;
        ensure!(
            !result.trim_start().starts_with("Code:") && !result.contains("DB::Exception:"),
            "ClickHouse reported a query exception after response headers"
        );
        Ok(result)
    }

    pub fn rows(&self, sql: &str, params: &Params) -> Result<Rows> {
        Ok(Rows {
            lines: BufReader::new(self.request(
                &format!("{sql} FORMAT JSONEachRow"),
                params,
                Vec::new(),
            )?)
            .lines(),
            failed: false,
        })
    }

    pub fn one(&self, sql: &str, params: &Params) -> Result<Value> {
        let mut rows = self.rows(sql, params)?;
        let row = rows.next().context("expected one result row, got none")??;
        if let Some(next) = rows.next() {
            next?;
            bail!("expected one result row, got multiple");
        }
        Ok(row)
    }

    pub fn insert(
        &self,
        table: &str,
        rows: impl IntoIterator<Item = Result<Value>>,
        batch_size: usize,
    ) -> Result<()> {
        identifier(table)?;
        ensure!(batch_size > 0, "insert batch size must be positive");
        let mut body = Vec::new();
        let mut count = 0;
        for row in rows {
            serde_json::to_writer(&mut body, &row?)?;
            body.push(b'\n');
            count += 1;
            if count == batch_size {
                self.insert_body(table, std::mem::take(&mut body))?;
                count = 0;
            }
        }
        if count != 0 {
            self.insert_body(table, body)?;
        }
        Ok(())
    }

    pub fn insert_values(&self, table: &str, rows: impl IntoIterator<Item = Value>) -> Result<()> {
        self.insert(table, rows.into_iter().map(Ok), 1000)
    }

    fn insert_body(&self, table: &str, body: Vec<u8>) -> Result<()> {
        let response = self.request(
            &format!("INSERT INTO {table} FORMAT JSONEachRow"),
            &Params::new(),
            body,
        )?;
        let mut acknowledgement = Vec::new();
        response
            .take(8193)
            .read_to_end(&mut acknowledgement)
            .context("incomplete ClickHouse insert response")?;
        ensure!(
            acknowledgement.len() <= 8192
                && acknowledgement.iter().all(|b| b.is_ascii_whitespace()),
            "ClickHouse returned an invalid insert acknowledgement"
        );
        Ok(())
    }

    pub fn disk_usage(&self) -> Result<u64> {
        uint(
            &self.one(
                "SELECT sum(bytes_on_disk) AS bytes FROM system.parts WHERE database = {db:String}",
                &params(serde_json::json!({"db":self.database}))?,
            )?["bytes"],
        )
    }
}

pub struct Rows {
    lines: Lines<BufReader<Response>>,
    failed: bool,
}
impl Iterator for Rows {
    type Item = Result<Value>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            let line = match self.lines.next()? {
                Ok(line) => line,
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error.into()));
                }
            };
            if line.trim().is_empty() {
                continue;
            }
            let parsed = serde_json::from_str::<Value>(&line)
                .context("invalid ClickHouse JSON row")
                .and_then(|value| {
                    ensure!(value.is_object(), "ClickHouse row must be an object");
                    Ok(value)
                });
            if parsed.is_err() {
                self.failed = true;
            }
            return Some(parsed);
        }
    }
}
