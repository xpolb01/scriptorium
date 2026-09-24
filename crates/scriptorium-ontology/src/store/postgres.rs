//! Postgres storage (build feature `postgres`): one table per entity type,
//! one column per field.
//!
//! - **Tables.** An entity type's table is `tables.<Type>` from the manifest,
//!   or the type name in `snake_case` (`PaperKind` → `paper_kind`), in
//!   `schema` (default `public`). It must be a table whose key column is
//!   declared unique and `NOT NULL` (a `PRIMARY KEY`): a view cannot
//!   guarantee unique keys, so `ready` refuses one.
//! - **Columns are fields.** A row is read as a JSON object of the entity's
//!   key column and the columns of the fields this sub-ontology stores
//!   (`jsonb_build_object`), so column names are the GraphQL field names,
//!   case included (`"createdAt"`), and a role needs `SELECT` on those
//!   columns only. The key column is the entity's key field. Relation columns hold target
//!   keys, as a scalar (`text`, `integer`) or an array (`text[]`,
//!   `integer[]`); `jsonb` columns can hold value objects.
//! - **Keys compare as text** (`("id")::text = $1`), like the other stores,
//!   so `integer` keys work; lists are ordered by the key's text in the
//!   database's collation. Inverse relations (`references`) scan the table.
//! - Every identifier is double-quoted (embedded quotes doubled) and every
//!   value is a query parameter.
//!
//! The connection string comes from the environment variable named by
//! `url_env`; it is never logged or put in an error. The store connects on
//! first use (`serve` calls [`EntityStore::ready`] at startup), shares one
//! connection between all queries, and reconnects once it has closed or a
//! query on it went unanswered for 30 s (its socket is then closed, not
//! left waiting). Connecting, each address tried gets `connect_timeout` (10 s
//! unless the connection string sets one), its name lookup, TLS, and the
//! startup exchange included, and one that fails or stalls gives way to the
//! next, as libpq does.
//!
//! **TLS.** Any server other than a local one (`localhost`, a loopback
//! address, a Unix socket) is reached over TLS (rustls), with its
//! certificate and name verified against the system trust store:
//! `sslmode=disable` is refused, and `prefer` (the default) is treated as
//! `require`, so a server without TLS is an error, never a silent
//! downgrade. A local server is reached without TLS unless the connection
//! string says `sslmode=require`, which then verifies the same way.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use tokio_postgres::config::{Host, LoadBalanceHosts, SslMode};
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Config, Row};

use super::{EntityStore, ListQuery, Record};
use crate::error::{Error, Result};
use crate::manifest::PostgresConfig;

/// Whether `$1` (a quoted `"schema"."table"`) exists, and whether it has a
/// column named `$2`.
/// For relation `$1` (a quoted `"schema"."table"`) and key column `$2`:
/// whether the relation exists, whether it has the column, its kind
/// (`r` table, `p` partitioned table, `v` view, …), whether the column is
/// `NOT NULL`, and whether a valid, live unique index covers exactly that
/// column (a failed `CREATE UNIQUE INDEX CONCURRENTLY` leaves an invalid
/// one that guarantees nothing).
const TABLE_CHECK: &str = "SELECT c.oid IS NOT NULL, a.attnum IS NOT NULL, c.relkind::text, \
     COALESCE(a.attnotnull, false), \
     EXISTS (SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid = c.oid AND i.indisunique \
     AND i.indisvalid AND i.indisready AND i.indislive \
     AND i.indnkeyatts = 1 AND i.indkey[0] = a.attnum AND i.indpred IS NULL) \
     FROM (SELECT to_regclass($1) AS oid) r \
     LEFT JOIN pg_catalog.pg_class c ON c.oid = r.oid \
     LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attname = $2 \
     AND a.attnum > 0 AND NOT a.attisdropped";

/// How long one connection attempt may take, the socket, TLS, and the
/// startup exchange together, when the connection string sets no
/// `connect_timeout`.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// How long one query may take. A server that stops answering (stalled, or
/// cut off behind a proxy that keeps the socket open) must not hold a
/// request, or `serve`'s startup, forever.
const QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

pub struct PostgresStore {
    sub: String,
    url_env: String,
    schema: String,
    /// Entity type → its table and the queries that read it.
    tables: BTreeMap<String, Table>,
    /// Entity types this sub-ontology stores nothing for (it extends them
    /// with derived fields only), so they need no table.
    unstored: BTreeSet<String>,
    /// The connection every query shares: opened on first use, replaced
    /// once closed or once a query on it got no answer.
    client: tokio::sync::Mutex<Option<Session>>,
    /// [`QUERY_TIMEOUT`]; shorter in tests.
    query_timeout: std::time::Duration,
}

/// An open connection: its client, and the task driving its socket.
struct Session {
    client: Arc<Client>,
    /// Aborted when a query on it gets no answer: dropping the client is not
    /// enough, since the task would wait for that answer, holding its socket,
    /// as long as the server stalls.
    driver: tokio::task::AbortHandle,
}

/// One entity type's table, and its queries.
struct Table {
    /// `"schema"."table"`.
    relation: String,
    /// The key column (the entity's key field), unquoted.
    key_field: String,
    /// `$1` key.
    get: String,
    /// `$1` limit (`NULL` for none).
    list: String,
    /// `$1` referenced key, `$2` field, `$3` limit (`NULL` for none).
    references: String,
}

/// A stored field's column, and how it is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    /// Values that name something exactly (keys, relations, `ID` fields)
    /// are read as text (`Some(true)` for a list), so a wide `numeric`
    /// keeps every digit instead of becoming a rounded float.
    pub text: Option<bool>,
}

impl Column {
    /// The SQL expression that reads this column for the row object.
    fn expression(&self) -> String {
        let column = format!("t.{}", quote_ident(&self.name));
        match self.text {
            None => column,
            Some(false) => format!("({column})::text"),
            Some(true) => format!(
                "CASE WHEN {column} IS NULL THEN NULL ELSE COALESCE((SELECT jsonb_agg(u.e::text ORDER BY u.o) FROM unnest({column}) WITH ORDINALITY AS u(e, o)), '[]'::jsonb) END"
            ),
        }
    }
}

impl Table {
    fn new(schema: &str, table: &str, key_field: &str, fields: &[Column]) -> Self {
        let relation = format!("{}.{}", quote_ident(schema), quote_ident(table));
        let key = format!("({})::text", quote_ident(key_field));
        // Only the declared columns: a role granted just those can serve the
        // table. The key is read as text, like a relation, so it names the
        // entity exactly. `jsonb_build_object` takes at most 100 arguments,
        // so the pairs go in chunks.
        let mut columns: Vec<Column> = vec![Column {
            name: key_field.to_string(),
            text: Some(false),
        }];
        for field in fields {
            if field.name != key_field && !columns.iter().any(|c| c.name == field.name) {
                columns.push(field.clone());
            }
        }
        columns.sort_by(|a, b| a.name.cmp(&b.name));
        let pairs: Vec<String> = columns
            .iter()
            .map(|c| format!("{}, {}", quote_literal(&c.name), c.expression()))
            .collect();
        let row = pairs
            .chunks(50)
            .map(|chunk| format!("jsonb_build_object({})", chunk.join(", ")))
            .collect::<Vec<_>>()
            .join(" || ");
        let row = row.as_str();
        // `ListQuery::matches`: the field equals the key, or is a list
        // containing it; only strings and numbers can be keys.
        let references = format!(
            "SELECT {row} FROM {relation} AS t WHERE EXISTS (\
             SELECT 1 FROM jsonb_array_elements(CASE jsonb_typeof({row} -> $2) \
             WHEN 'array' THEN {row} -> $2 ELSE jsonb_build_array({row} -> $2) END) AS e(v) \
             WHERE jsonb_typeof(e.v) IN ('string', 'number') AND e.v #>> '{{}}' = $1) \
             ORDER BY {key} LIMIT $3"
        );
        Self {
            // Two, so a duplicate key is an error rather than a coin toss.
            get: format!("SELECT {row} FROM {relation} AS t WHERE {key} = $1 LIMIT 2"),
            list: format!("SELECT {row} FROM {relation} AS t ORDER BY {key} LIMIT $1"),
            references,
            relation,
            key_field: key_field.to_string(),
        }
    }
}

impl PostgresStore {
    /// A store for sub-ontology `sub`. Does no I/O: the connection opens on
    /// first use.
    pub fn new(
        sub: &str,
        config: &PostgresConfig,
        key_fields: BTreeMap<String, String>,
    ) -> Result<Self> {
        let stored = key_fields.keys().cloned().collect();
        Self::for_types(sub, config, key_fields, &stored, &BTreeMap::new())
    }

    /// Like [`PostgresStore::new`], but only the `stored` entity types have
    /// tables; the others read as empty without a query.
    pub fn for_types(
        sub: &str,
        config: &PostgresConfig,
        key_fields: BTreeMap<String, String>,
        stored: &BTreeSet<String>,
        fields: &BTreeMap<String, Vec<Column>>,
    ) -> Result<Self> {
        let invalid = |message: String| Error::Store {
            store: format!("postgres:{sub}"),
            message,
        };
        if config.url_env.trim().is_empty() {
            return Err(invalid(
                "`url_env` must name the environment variable holding the connection string".into(),
            ));
        }
        check_identifier("schema", &config.schema).map_err(invalid)?;
        if let Some(type_name) = config.tables.keys().find(|t| !key_fields.contains_key(*t)) {
            return Err(invalid(format!(
                "`tables` maps `{type_name}`, which is not an entity type of this sub-ontology"
            )));
        }
        if let Some(type_name) = config.tables.keys().find(|t| !stored.contains(*t)) {
            return Err(invalid(format!(
                "`tables` maps `{type_name}`, but this sub-ontology stores no fields of it"
            )));
        }
        let mut tables = BTreeMap::new();
        let mut unstored = BTreeSet::new();
        // Relation → the type reading it: two types on one table would read
        // each other's rows.
        let mut readers: BTreeMap<String, String> = BTreeMap::new();
        for (type_name, key_field) in key_fields {
            if !stored.contains(&type_name) {
                unstored.insert(type_name);
                continue;
            }
            let table = table_name(&type_name, &config.tables);
            check_identifier(&format!("the table for `{type_name}`"), &table).map_err(invalid)?;
            let columns = fields.get(&type_name).map_or(&[][..], Vec::as_slice);
            for column in
                std::iter::once(key_field.as_str()).chain(columns.iter().map(|c| c.name.as_str()))
            {
                check_identifier(&format!("the column `{type_name}.{column}`"), column)
                    .map_err(invalid)?;
            }
            let table = Table::new(
                &config.schema,
                &table,
                &key_field,
                fields.get(&type_name).map_or(&[][..], Vec::as_slice),
            );
            if let Some(other) = readers.insert(table.relation.clone(), type_name.clone()) {
                return Err(invalid(format!(
                    "`{other}` and `{type_name}` would both read {}; map one of them to another table in `[storage.tables]`",
                    table.relation
                )));
            }
            tables.insert(type_name, table);
        }
        Ok(Self {
            sub: sub.to_string(),
            url_env: config.url_env.clone(),
            schema: config.schema.clone(),
            tables,
            unstored,
            client: tokio::sync::Mutex::new(None),
            query_timeout: QUERY_TIMEOUT,
        })
    }

    fn error(&self, message: impl Into<String>) -> Error {
        Error::Store {
            store: self.describe(),
            message: message.into(),
        }
    }

    /// The table holding `type_name`, or `None` for a type this
    /// sub-ontology stores nothing for.
    fn table(&self, type_name: &str) -> Result<Option<&Table>> {
        if let Some(table) = self.tables.get(type_name) {
            return Ok(Some(table));
        }
        if self.unstored.contains(type_name) {
            return Ok(None);
        }
        Err(self.error(format!(
            "`{type_name}` is not an entity type of sub-ontology `{}`",
            self.sub
        )))
    }

    /// The shared connection, (re)connecting when there is none or it has
    /// closed. Concurrent callers wait for one connection attempt.
    async fn client(&self) -> Result<Arc<Client>> {
        let mut slot = self.client.lock().await;
        if let Some(session) = slot.as_ref().filter(|s| !s.client.is_closed()) {
            return Ok(Arc::clone(&session.client));
        }
        let session = self.connect().await?;
        let client = Arc::clone(&session.client);
        *slot = Some(session);
        Ok(client)
    }

    async fn connect(&self) -> Result<Session> {
        let url = std::env::var(&self.url_env).map_err(|e| {
            self.error(match e {
                std::env::VarError::NotPresent => format!(
                    "environment variable `{}` is not set; set it to the Postgres connection string",
                    self.url_env
                ),
                std::env::VarError::NotUnicode(_) => {
                    format!("environment variable `{}` is not valid UTF-8", self.url_env)
                }
            })
        })?;
        self.connect_to(&url).await
    }

    async fn connect_to(&self, url: &str) -> Result<Session> {
        let config = connection_config(&self.url_env, url).map_err(|m| self.error(m))?;
        self.connect_with(&config).await
    }

    /// Try each place `config` lists, as libpq does: every address gets
    /// `connect_timeout`, its name lookup, TLS, and the startup exchange
    /// included, and one that fails or stalls gives way to the next.
    /// (tokio-postgres bounds only the socket connect, and moves on only when
    /// that fails, so each attempt is one address of its own.)
    async fn connect_with(&self, config: &Config) -> Result<Session> {
        let all = destinations(config)
            .map_err(|why| self.error(format!("connecting with `{}`: {why}", self.url_env)))?;
        self.try_each(config, all).await
    }

    /// [`Self::connect_with`] over `destinations`, with `config`'s options.
    async fn try_each(&self, config: &Config, destinations: Vec<Destination>) -> Result<Session> {
        let limit = config
            .get_connect_timeout()
            .copied()
            .unwrap_or(CONNECT_TIMEOUT);
        let random = config.get_load_balance_hosts() == LoadBalanceHosts::Random;
        let mut last = String::from("no destination to try");
        for destination in destinations {
            // A host's name lookup counts toward its first address's time.
            let started = tokio::time::Instant::now();
            let addresses = match addresses(&destination, limit, random).await {
                Ok(addresses) => addresses,
                Err(why) => {
                    last = why;
                    continue;
                }
            };
            for (n, address) in addresses.into_iter().enumerate() {
                let deadline = if n == 0 {
                    started + limit
                } else {
                    tokio::time::Instant::now() + limit
                };
                let one =
                    one_destination(config, destination.host.as_ref(), address, destination.port);
                match tokio::time::timeout_at(deadline, self.open(&one)).await {
                    Ok(Ok(session)) => return Ok(session),
                    Ok(Err(why)) => last = why,
                    Err(_) => {
                        last = format!(
                            "no answer within {}s; set `connect_timeout` in the connection string to wait longer",
                            limit.as_secs()
                        );
                    }
                }
            }
        }
        Err(self.error(format!("connecting with `{}`: {last}", self.url_env)))
    }

    /// Connect to the one place `config` lists.
    async fn open(&self, config: &Config) -> std::result::Result<Session, String> {
        let store = self.describe();
        if config.get_ssl_mode() == SslMode::Require {
            let tls = tls_connector()?;
            let (client, connection) = config.connect(tls).await.map_err(|e| error_chain(&e))?;
            Ok(Session {
                client: Arc::new(client),
                driver: drive(store, connection),
            })
        } else {
            // Only a local server gets here (see `connection_config`).
            let (client, connection) = config
                .connect(tokio_postgres::NoTls)
                .await
                .map_err(|e| error_chain(&e))?;
            Ok(Session {
                client: Arc::new(client),
                driver: drive(store, connection),
            })
        }
    }

    /// Wait for `query` over `client` for at most `query_timeout`. With no
    /// answer by then, the request fails and the connection is closed, so
    /// the next query reconnects instead of queueing behind this one.
    async fn within<T>(
        &self,
        client: &Client,
        what: &str,
        query: impl std::future::Future<Output = std::result::Result<T, tokio_postgres::Error>>,
    ) -> Result<std::result::Result<T, tokio_postgres::Error>> {
        if let Ok(answered) = tokio::time::timeout(self.query_timeout, query).await {
            return Ok(answered);
        }
        let mut slot = self.client.lock().await;
        if let Some(stalled) = slot.take_if(|s| std::ptr::eq(Arc::as_ptr(&s.client), client)) {
            stalled.driver.abort();
        }
        Err(self.error(format!(
            "{what}: no answer within {}s",
            self.query_timeout.as_secs()
        )))
    }

    fn query_error(&self, table: &Table, e: &tokio_postgres::Error) -> Error {
        self.error(format!("reading {}: {}", table.relation, error_chain(e)))
    }

    fn record(&self, table: &Table, row: &Row) -> Result<Record> {
        match row.try_get::<_, serde_json::Value>(0) {
            Ok(serde_json::Value::Object(record)) => match record.get(&table.key_field) {
                Some(serde_json::Value::String(_) | serde_json::Value::Number(_)) => Ok(record),
                _ => Err(self.error(format!(
                    "reading {}: a row has no `{}` (null, or not a string or number); every row needs its key",
                    table.relation, table.key_field
                ))),
            },
            Ok(other) => Err(self.error(format!(
                "reading {}: expected a row object, got {}",
                table.relation,
                crate::store::git::json_kind(&other)
            ))),
            Err(e) => Err(self.query_error(table, &e)),
        }
    }
}

#[async_trait]
impl EntityStore for PostgresStore {
    fn describe(&self) -> String {
        format!("postgres:{}@${}", self.schema, self.url_env)
    }

    async fn get(&self, type_name: &str, key: &str) -> Result<Option<Arc<Record>>> {
        let Some(table) = self.table(type_name)? else {
            return Ok(None);
        };
        if key.contains('\0') {
            // Postgres text cannot hold NUL, so no row has this key.
            return Ok(None);
        }
        let client = self.client().await?;
        let rows = self
            .within(
                &client,
                &format!("reading {}", table.relation),
                client.query_typed(&table.get, &[(&key, Type::TEXT)]),
            )
            .await?
            .map_err(|e| self.query_error(table, &e))?;
        match rows.as_slice() {
            [] => Ok(None),
            [row] => self.record(table, row).map(|record| Some(Arc::new(record))),
            _ => Err(self.error(format!(
                "reading {}: more than one row has key `{key}`; the key column must be unique",
                table.relation
            ))),
        }
    }

    async fn list(&self, type_name: &str, query: &ListQuery) -> Result<Vec<Arc<Record>>> {
        let Some(table) = self.table(type_name)? else {
            return Ok(Vec::new());
        };
        let limit = query.limit.map(|n| i64::try_from(n).unwrap_or(i64::MAX));
        if query
            .references
            .as_ref()
            .is_some_and(|(_, key)| key.contains('\0'))
        {
            return Ok(Vec::new());
        }
        let client = self.client().await?;
        let what = format!("reading {}", table.relation);
        let rows = match &query.references {
            None => {
                self.within(
                    &client,
                    &what,
                    client.query_typed(&table.list, &[(&limit, Type::INT8)]),
                )
                .await?
            }
            Some((field, key)) => {
                self.within(
                    &client,
                    &what,
                    client.query_typed(
                        &table.references,
                        &[(key, Type::TEXT), (field, Type::TEXT), (&limit, Type::INT8)],
                    ),
                )
                .await?
            }
        }
        .map_err(|e| self.query_error(table, &e))?;
        let records = rows
            .iter()
            .map(|row| self.record(table, row))
            .collect::<Result<Vec<_>>>()?;
        let mut keys = std::collections::HashSet::new();
        for record in &records {
            let key = record
                .get(&table.key_field)
                .map(key_text)
                .unwrap_or_default();
            if !keys.insert(key.clone()) {
                return Err(self.error(format!(
                    "reading {}: more than one row has key `{key}`; the key column must be unique",
                    table.relation
                )));
            }
        }
        Ok(records.into_iter().map(Arc::new).collect())
    }

    async fn ready(&self) -> Result<()> {
        let client = self.client().await?;
        self.check_tables(&client).await
    }
}

impl PostgresStore {
    /// `ready`'s checks over `client`: every table exists, is a table, has
    /// its key declared unique and `NOT NULL`, and can be read.
    async fn check_tables(&self, client: &Client) -> Result<()> {
        let mut missing_tables = Vec::new();
        let mut missing_keys = Vec::new();
        let mut loose_keys = Vec::new();
        let mut not_tables = Vec::new();
        let mut unreadable = Vec::new();
        for (type_name, table) in &self.tables {
            let what = format!("checking {}", table.relation);
            let row = self
                .within(
                    client,
                    &what,
                    client.query_typed_one(
                        TABLE_CHECK,
                        &[
                            (&table.relation, Type::TEXT),
                            (&table.key_field, Type::TEXT),
                        ],
                    ),
                )
                .await?
                .map_err(|e| {
                    self.error(format!("checking {}: {}", table.relation, error_chain(&e)))
                })?;
            let (exists, has_key): (bool, bool) = (row.get(0), row.get(1));
            let kind: Option<String> = row.get(2);
            let (not_null, unique): (bool, bool) = (row.get(3), row.get(4));
            let column = format!(
                "{}.{} (`{type_name}`)",
                table.relation,
                quote_ident(&table.key_field)
            );
            if !exists {
                missing_tables.push(format!("{} (`{type_name}`)", table.relation));
            } else if !has_key {
                missing_keys.push(column);
            } else if !matches!(kind.as_deref(), Some("r" | "p")) {
                // A view cannot declare a key, and a LIMIT would hide the
                // duplicates it lets through.
                not_tables.push(format!("{} (`{type_name}`)", table.relation));
            } else if !(not_null && unique) {
                loose_keys.push(column);
            } else if let Err(e) = self
                .within(
                    client,
                    &what,
                    client.query_typed(&table.list, &[(&Some(0_i64), Type::INT8)]),
                )
                .await?
            {
                // The catalogs are readable by any role; the rows may not be.
                unreadable.push(format!(
                    "{} (`{type_name}`): {}",
                    table.relation,
                    error_chain(&e)
                ));
            }
        }
        let mut problems = Vec::new();
        if !missing_tables.is_empty() {
            problems.push(format!("missing tables: {}", missing_tables.join(", ")));
        }
        if !missing_keys.is_empty() {
            problems.push(format!("missing key columns: {}", missing_keys.join(", ")));
        }
        if !loose_keys.is_empty() {
            problems.push(format!(
                "key columns not declared unique and NOT NULL (add a PRIMARY KEY): {}",
                loose_keys.join(", ")
            ));
        }
        if !not_tables.is_empty() {
            problems.push(format!(
                "not tables (a view cannot guarantee unique keys; store the type in a table): {}",
                not_tables.join(", ")
            ));
        }
        if !unreadable.is_empty() {
            problems.push(format!("cannot read {}", unreadable.join(", ")));
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(self.error(problems.join("; ")))
        }
    }
}

/// The table an entity type is stored in: its `tables` entry, or its name
/// in `snake_case`.
fn table_name(type_name: &str, tables: &BTreeMap<String, String>) -> String {
    tables
        .get(type_name)
        .cloned()
        .unwrap_or_else(|| snake_case(type_name))
}

/// `PaperKind` → `paper_kind`, `HTTPRequest` → `http_request`.
fn snake_case(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut out = String::with_capacity(name.len() + 4);
    for (i, &c) in chars.iter().enumerate() {
        if !c.is_ascii_uppercase() {
            out.push(c);
            continue;
        }
        let prev = i.checked_sub(1).map(|j| chars[j]);
        let next = chars.get(i + 1);
        let starts_word = match prev {
            Some(p) if p.is_ascii_lowercase() || p.is_ascii_digit() => true,
            // The last capital of an acronym starts the next word: `HTTPRequest`.
            Some(p) if p.is_ascii_uppercase() => next.is_some_and(char::is_ascii_lowercase),
            _ => false,
        };
        if starts_word {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// A double-quoted identifier, embedded quotes doubled: taken literally,
/// case included.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The longest identifier Postgres keeps whole (`NAMEDATALEN` - 1).
const MAX_IDENTIFIER: usize = 63;

/// Postgres refuses empty quoted identifiers and NUL characters, and cuts
/// longer ones to [`MAX_IDENTIFIER`] bytes, where two names can meet.
fn check_identifier(what: &str, name: &str) -> std::result::Result<(), String> {
    if name.is_empty() {
        Err(format!("{what} must not be empty"))
    } else if name.contains('\0') {
        Err(format!("{what} must not contain NUL characters"))
    } else if name.len() > MAX_IDENTIFIER {
        Err(format!(
            "{what} is {} bytes, but Postgres cuts identifiers to {MAX_IDENTIFIER}, where two names could read one table or column",
            name.len()
        ))
    } else {
        Ok(())
    }
}

/// `value` as an SQL string literal.
fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// A key as the text it is compared as.
fn key_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Parse the connection string held by `url_env`, and settle how it may
/// connect (see the module docs on TLS). Errors never quote it.
fn connection_config(url_env: &str, url: &str) -> std::result::Result<Config, String> {
    let mut config: Config = url.parse().map_err(|e: tokio_postgres::Error| {
        // Parse errors can quote pieces of the input; only those naming an
        // option (never its value) are shown.
        match std::error::Error::source(&e).map(ToString::to_string) {
            // `verify-ca`, `verify-full`, …
            Some(reason) if reason == "invalid value for option `sslmode`" => format!(
                "`{url_env}` sets an `sslmode` this client does not know; use `sslmode=require` \
                 (which verifies the server's certificate and name against the system trust store)"
            ),
            Some(reason) if reason.starts_with("invalid value for option `") => {
                format!("`{url_env}` does not hold a valid Postgres connection string: {reason}")
            }
            _ => format!(
                "`{url_env}` does not hold a valid Postgres connection string (details withheld: they could quote it)"
            ),
        }
    })?;
    // What is contacted: each `hostaddr` in place of its `host`, when set.
    let (hosts, addrs) = (config.get_hosts(), config.get_hostaddrs());
    let destinations = hosts.len().max(addrs.len());
    let local = destinations > 0
        && (0..destinations).all(|i| match addrs.get(i) {
            Some(addr) => addr.is_loopback(),
            None => hosts.get(i).is_some_and(is_local),
        });
    // Bounds each socket connect; `connect_to` holds TLS and the startup
    // exchange to it as well.
    if config.get_connect_timeout().is_none() {
        config.connect_timeout(CONNECT_TIMEOUT);
    }
    if !local {
        match config.get_ssl_mode() {
            SslMode::Disable => {
                return Err(format!(
                    "`{url_env}` disables TLS (`sslmode=disable`), which is allowed only for a \
                     local server (localhost, a loopback address, or a Unix socket)"
                ));
            }
            // No silent plaintext fallback beyond this machine.
            SslMode::Prefer => {
                config.ssl_mode(SslMode::Require);
            }
            _ => {}
        }
    }
    Ok(config)
}

fn is_local(host: &Host) -> bool {
    match host {
        Host::Tcp(name) => {
            let name = name.trim_start_matches('[').trim_end_matches(']');
            name.eq_ignore_ascii_case("localhost")
                || name
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        }
        #[cfg(unix)]
        Host::Unix(_) => true,
    }
}

/// Run a connection's I/O in the background until it closes (or the
/// returned handle aborts it).
fn drive<S, T>(
    store: String,
    connection: tokio_postgres::Connection<S, T>,
) -> tokio::task::AbortHandle
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!(store = %store, error = %error_chain(&e), "postgres connection closed");
        }
    })
    .abort_handle()
}

/// One place a connection string lists: a host (kept for TLS names), the
/// address to use instead of looking the host up, and the port.
struct Destination {
    host: Option<Host>,
    hostaddr: Option<std::net::IpAddr>,
    port: u16,
}

/// The places `config` lists, paired as tokio-postgres pairs them: host
/// and `hostaddr` by position, and one port for every host unless each has
/// its own. Shuffled for `load_balance_hosts=random`.
fn destinations(config: &Config) -> std::result::Result<Vec<Destination>, String> {
    let (hosts, addrs, ports) = (
        config.get_hosts(),
        config.get_hostaddrs(),
        config.get_ports(),
    );
    if hosts.is_empty() && addrs.is_empty() {
        return Err("names no host".into());
    }
    if !hosts.is_empty() && !addrs.is_empty() && hosts.len() != addrs.len() {
        return Err(format!(
            "lists {} hosts but {} hostaddrs",
            hosts.len(),
            addrs.len()
        ));
    }
    let count = hosts.len().max(addrs.len());
    if ports.len() > 1 && ports.len() != count {
        return Err(format!("lists {} ports for {count} hosts", ports.len()));
    }
    let mut all: Vec<Destination> = (0..count)
        .map(|i| Destination {
            host: hosts.get(i).cloned(),
            hostaddr: addrs.get(i).copied(),
            port: ports
                .get(i)
                .or_else(|| ports.first())
                .copied()
                .unwrap_or(5432),
        })
        .collect();
    if config.get_load_balance_hosts() == LoadBalanceHosts::Random {
        shuffle(&mut all);
    }
    Ok(all)
}

/// The addresses to try for `destination`: its `hostaddr`; each address a
/// host name resolves to, the lookup held to `limit` too; or, for a socket
/// path, `None`. Errors never name the host.
async fn addresses(
    destination: &Destination,
    limit: std::time::Duration,
    random: bool,
) -> std::result::Result<Vec<Option<std::net::IpAddr>>, String> {
    if let Some(addr) = destination.hostaddr {
        return Ok(vec![Some(addr)]);
    }
    let Some(Host::Tcp(name)) = &destination.host else {
        return Ok(vec![None]);
    };
    let lookup = tokio::net::lookup_host((name.as_str(), destination.port));
    let mut found: Vec<Option<std::net::IpAddr>> = match tokio::time::timeout(limit, lookup).await {
        Ok(Ok(found)) => found.map(|a| Some(a.ip())).collect(),
        Ok(Err(e)) => return Err(format!("looking up a host: {e}")),
        Err(_) => {
            return Err(format!(
                "looking up a host: no answer within {}s",
                limit.as_secs()
            ))
        }
    };
    if found.is_empty() {
        return Err("looking up a host: it has no address".into());
    }
    if random {
        shuffle(&mut found);
    }
    Ok(found)
}

/// `config` with one destination only: `host` (so TLS verifies its name),
/// `hostaddr`, and `port`; every other option as it was.
fn one_destination(
    config: &Config,
    host: Option<&Host>,
    hostaddr: Option<std::net::IpAddr>,
    port: u16,
) -> Config {
    let mut one = Config::new();
    if let Some(user) = config.get_user() {
        one.user(user);
    }
    if let Some(password) = config.get_password() {
        one.password(password);
    }
    if let Some(dbname) = config.get_dbname() {
        one.dbname(dbname);
    }
    if let Some(options) = config.get_options() {
        one.options(options);
    }
    if let Some(name) = config.get_application_name() {
        one.application_name(name);
    }
    if let Some(timeout) = config.get_connect_timeout() {
        one.connect_timeout(*timeout);
    }
    if let Some(timeout) = config.get_tcp_user_timeout() {
        one.tcp_user_timeout(*timeout);
    }
    if let Some(interval) = config.get_keepalives_interval() {
        one.keepalives_interval(interval);
    }
    if let Some(retries) = config.get_keepalives_retries() {
        one.keepalives_retries(retries);
    }
    one.ssl_mode(config.get_ssl_mode())
        .ssl_negotiation(config.get_ssl_negotiation())
        .keepalives(config.get_keepalives())
        .keepalives_idle(config.get_keepalives_idle())
        .target_session_attrs(config.get_target_session_attrs())
        .channel_binding(config.get_channel_binding())
        .port(port);
    match host {
        Some(Host::Tcp(name)) => {
            one.host(name);
        }
        #[cfg(unix)]
        Some(Host::Unix(path)) => {
            one.host_path(path);
        }
        None => {}
    }
    if let Some(addr) = hostaddr {
        one.hostaddr(addr);
    }
    one
}

/// Shuffle `items` in place (Fisher-Yates), for `load_balance_hosts=random`.
fn shuffle<T>(items: &mut [T]) {
    use std::hash::{BuildHasher, Hasher};
    for i in (1..items.len()).rev() {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_usize(i);
        let j = usize::try_from(hasher.finish() % (i as u64 + 1)).unwrap_or(0);
        items.swap(i, j);
    }
}

/// TLS through rustls (aws-lc-rs), verifying servers against the system
/// trust store.
fn tls_connector() -> std::result::Result<tokio_postgres_rustls::MakeRustlsConnect, String> {
    use rustls_platform_verifier::BuilderVerifierExt;

    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .and_then(BuilderVerifierExt::with_platform_verifier)
        .map_err(|e| format!("setting up TLS: {e}"))?
        .with_no_client_auth();
    Ok(tokio_postgres_rustls::MakeRustlsConnect::new(config))
}

/// `e` and its causes: tokio-postgres keeps the detail (the server's
/// message, the I/O error) in `source()`.
fn error_chain(e: &dyn std::error::Error) -> String {
    let mut message = e.to_string();
    let mut cause = e.source();
    while let Some(c) = cause {
        message.push_str(": ");
        message.push_str(&c.to_string());
        cause = c.source();
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value as Json};

    fn config(schema: &str, tables: &[(&str, &str)]) -> PostgresConfig {
        PostgresConfig {
            url_env: "DB_URL".into(),
            schema: schema.into(),
            tables: pairs(tables),
        }
    }

    fn pairs(items: &[(&str, &str)]) -> BTreeMap<String, String> {
        items
            .iter()
            .map(|(a, b)| ((*a).to_string(), (*b).to_string()))
            .collect()
    }

    #[test]
    fn identifiers_are_quoted_with_embedded_quotes_doubled() {
        assert_eq!(quote_ident("paper"), r#""paper""#);
        assert_eq!(quote_ident("Paper Kind"), r#""Paper Kind""#);
        assert_eq!(quote_ident(r#"a"b"#), r#""a""b""#);
        assert_eq!(quote_ident(r#"""#), r#""""""#);
        assert_eq!(
            quote_ident(r#"x"; DROP TABLE y; --"#),
            r#""x""; DROP TABLE y; --""#
        );
    }

    #[test]
    fn type_names_become_snake_case_tables() {
        for (name, table) in [
            ("Paper", "paper"),
            ("PaperKind", "paper_kind"),
            ("HTTPRequest", "http_request"),
            ("APIKey", "api_key"),
            ("ID", "id"),
            ("Paper2Code", "paper2_code"),
            ("already_snake", "already_snake"),
            ("_Private", "_private"),
        ] {
            assert_eq!(snake_case(name), table, "{name}");
        }
    }

    #[test]
    fn tables_come_from_the_manifest_or_the_type_name() {
        let tables = pairs(&[("Paper", "papers")]);
        assert_eq!(table_name("Paper", &tables), "papers");
        assert_eq!(table_name("PaperKind", &tables), "paper_kind");

        let keys = pairs(&[("Paper", "paperId"), ("PaperKind", "id")]);
        let stored = keys.keys().cloned().collect();
        let fields = BTreeMap::from([(
            "Paper".to_string(),
            vec![Column {
                name: "title".into(),
                text: None,
            }],
        )]);
        let store = PostgresStore::for_types(
            "research",
            &config(r#"my "schema""#, &[("Paper", "Papers")]),
            keys,
            &stored,
            &fields,
        )
        .unwrap();
        let paper = &store.tables["Paper"];
        assert_eq!(paper.relation, r#""my ""schema"""."Papers""#);
        // Only the key and the declared fields are read.
        assert_eq!(
            paper.get,
            r#"SELECT jsonb_build_object('paperId', (t."paperId")::text, 'title', t."title") FROM "my ""schema"""."Papers" AS t WHERE ("paperId")::text = $1 LIMIT 2"#
        );
        assert_eq!(
            paper.list,
            r#"SELECT jsonb_build_object('paperId', (t."paperId")::text, 'title', t."title") FROM "my ""schema"""."Papers" AS t ORDER BY ("paperId")::text LIMIT $1"#
        );
        let row = r#"jsonb_build_object('id', (t."id")::text)"#;
        assert_eq!(
            store.tables["PaperKind"].references,
            format!(
                r#"SELECT {row} FROM "my ""schema"""."paper_kind" AS t WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(CASE jsonb_typeof({row} -> $2) WHEN 'array' THEN {row} -> $2 ELSE jsonb_build_array({row} -> $2) END) AS e(v) WHERE jsonb_typeof(e.v) IN ('string', 'number') AND e.v #>> '{{}}' = $1) ORDER BY ("id")::text LIMIT $3"#
            )
        );
        // `jsonb_build_object` takes at most 100 arguments: 60 fields go in two calls.
        let many: Vec<Column> = (0..60)
            .map(|i| Column {
                name: format!("f{i:02}"),
                text: None,
            })
            .collect();
        let wide = Table::new("public", "wide", "id", &many);
        assert_eq!(
            wide.get.matches("jsonb_build_object(").count(),
            2,
            "{}",
            wide.get
        );
    }

    #[test]
    fn invalid_configs_are_rejected_without_io() {
        let keys = || pairs(&[("Paper", "id")]);
        let mut empty_env = config("public", &[]);
        empty_env.url_env = " ".into();
        for (bad, expected) in [
            (empty_env, "`url_env` must name"),
            (config("", &[]), "schema must not be empty"),
            (config("a\0b", &[]), "schema must not contain NUL"),
            (
                config("public", &[("Paper", "")]),
                "the table for `Paper` must not be empty",
            ),
            (
                config("public", &[("Papr", "papers")]),
                "`tables` maps `Papr`",
            ),
        ] {
            let err = PostgresStore::new("research", &bad, keys())
                .err()
                .unwrap()
                .to_string();
            assert!(err.contains(expected), "{err}");
            assert!(err.starts_with("store `postgres:research`"), "{err}");
        }
    }

    #[test]
    fn remote_hosts_need_tls() {
        let mode = |url: &str| connection_config("DB_URL", url).map(|c| c.get_ssl_mode());
        // Beyond this machine, `prefer` means `require`, and `disable` is refused.
        for url in [
            "postgres://alice:hunter2@db.internal/app",
            "postgres://alice:hunter2@db.internal/app?sslmode=prefer",
            "host=localhost,db.internal password=hunter2",
            // `hostaddr` is what is contacted, whatever `host` says.
            "host=localhost hostaddr=10.0.0.1 password=hunter2",
            "hostaddr=10.0.0.1 password=hunter2",
        ] {
            assert_eq!(mode(url), Ok(SslMode::Require), "{url}");
        }
        assert_eq!(
            mode("postgres://alice:hunter2@db.internal/app?sslmode=require"),
            Ok(SslMode::Require)
        );
        for url in [
            "postgres://alice:hunter2@db.internal/app?sslmode=disable",
            "host=db.internal password=hunter2 sslmode=disable",
            "host=localhost hostaddr=10.0.0.1 password=hunter2 sslmode=disable",
        ] {
            let err = mode(url).unwrap_err();
            assert!(err.contains("allowed only for a local server"), "{err}");
            assert!(
                !err.contains("hunter2") && !err.contains("db.internal"),
                "{err}"
            );
        }
        // A local server may go without TLS.
        for url in [
            "postgres://alice:hunter2@localhost/app",
            "postgres://alice:hunter2@127.0.0.1:5432/app?sslmode=disable",
            "postgres://alice:hunter2@[::1]/app",
            "host=/var/run/postgresql user=alice sslmode=disable",
            "host=db.internal hostaddr=127.0.0.1 user=alice sslmode=disable",
        ] {
            assert!(
                matches!(mode(url), Ok(SslMode::Prefer | SslMode::Disable)),
                "{url}: {:?}",
                mode(url)
            );
        }
        let err = mode("postgres://alice:hunter2@db.internal/app?sslmode=verify-full").unwrap_err();
        assert!(err.contains("use `sslmode=require`"), "{err}");
        assert!(!err.contains("hunter2"), "{err}");
    }

    #[tokio::test]
    async fn untrusted_server_certificates_are_refused() {
        // A TLS server whose certificate the system does not trust, e.g.
        // postgres with `ssl=on` and a self-signed certificate.
        let Ok(url) = std::env::var("SCRIPTORIUM_ONTOLOGY_TEST_POSTGRES_UNTRUSTED_TLS_URL") else {
            skip("set SCRIPTORIUM_ONTOLOGY_TEST_POSTGRES_UNTRUSTED_TLS_URL");
            return;
        };
        let store =
            PostgresStore::new("research", &config("public", &[]), BTreeMap::new()).unwrap();
        let err = store.connect_to(&url).await.err().unwrap().to_string();
        eprintln!("refused as expected: {err}");
        assert!(err.to_lowercase().contains("certificate"), "{err}");
    }

    #[test]
    fn connections_wait_a_bounded_time() {
        let config = connection_config("DB_URL", "postgres://u@localhost/db").unwrap();
        assert_eq!(config.get_connect_timeout(), Some(&CONNECT_TIMEOUT));
        let config =
            connection_config("DB_URL", "postgres://u@localhost/db?connect_timeout=3").unwrap();
        assert_eq!(
            config.get_connect_timeout(),
            Some(&std::time::Duration::from_secs(3))
        );
    }

    #[test]
    fn destinations_pair_hosts_addresses_and_ports() {
        let places = |url: &str| {
            destinations(&url.parse::<Config>().unwrap()).map(|all| {
                all.into_iter()
                    .map(|d| {
                        let host = match d.host {
                            Some(Host::Tcp(name)) => name,
                            _ => "-".into(),
                        };
                        let addr = d.hostaddr.map_or("-".into(), |a| a.to_string());
                        format!("{host}/{addr}:{}", d.port)
                    })
                    .collect::<Vec<_>>()
            })
        };
        assert_eq!(places("host=a,b port=1,2").unwrap(), ["a/-:1", "b/-:2"]);
        assert_eq!(places("host=a,b port=7").unwrap(), ["a/-:7", "b/-:7"]);
        assert_eq!(places("host=a").unwrap(), ["a/-:5432"]);
        assert_eq!(
            places("host=a,b hostaddr=10.0.0.1,10.0.0.2").unwrap(),
            ["a/10.0.0.1:5432", "b/10.0.0.2:5432"]
        );
        assert_eq!(places("hostaddr=10.0.0.1").unwrap(), ["-/10.0.0.1:5432"]);
        assert!(places("host=a,b,c port=1,2").is_err());
        assert!(places("host=a,b hostaddr=10.0.0.1").is_err());
    }

    #[test]
    fn one_destination_keeps_every_other_option() {
        let options = "user=u password=p dbname=d options=-cgeqo=off application_name=app \
                       sslmode=require sslnegotiation=direct connect_timeout=3 keepalives=0 \
                       keepalives_idle=30 keepalives_interval=5 keepalives_retries=4 \
                       target_session_attrs=read-write channel_binding=require";
        let all: Config = format!(
            "host=a,b hostaddr=10.0.0.1,10.0.0.2 port=1,2 load_balance_hosts=random {options}"
        )
        .parse()
        .unwrap();
        let expected: Config = format!("host=b hostaddr=10.0.0.2 port=2 {options}")
            .parse()
            .unwrap();
        let one = one_destination(
            &all,
            Some(&Host::Tcp("b".into())),
            Some("10.0.0.2".parse().unwrap()),
            2,
        );
        // Debug shows every option but the password and `sslnegotiation`.
        assert_eq!(format!("{one:?}"), format!("{expected:?}"));
        assert_eq!(one.get_password(), expected.get_password());
        assert_eq!(one.get_ssl_negotiation(), expected.get_ssl_negotiation());
    }

    /// Accepts connections on a port of its own and never answers.
    async fn silent_server() -> (u16, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        (port, task)
    }

    #[tokio::test]
    async fn each_stalled_host_gets_its_own_connect_timeout() {
        let ((first, a), (second, b)) = (silent_server().await, silent_server().await);
        let store =
            PostgresStore::new("research", &config("public", &[]), BTreeMap::new()).unwrap();
        let url = format!(
            "host=127.0.0.1,127.0.0.1 port={first},{second} user=alice password=hunter2 connect_timeout=1"
        );
        let started = std::time::Instant::now();
        let err = tokio::time::timeout(std::time::Duration::from_secs(10), store.connect_to(&url))
            .await
            .expect("the attempt ends on its own")
            .err()
            .unwrap()
            .to_string();
        let took = started.elapsed();
        assert!(err.contains("no answer within 1s"), "{err}");
        // Both were tried, a second each.
        assert!(took >= std::time::Duration::from_millis(1800), "{took:?}");
        assert!(took < std::time::Duration::from_secs(6), "{took:?}");
        a.abort();
        b.abort();
    }

    #[tokio::test]
    async fn a_server_that_never_answers_is_given_up_on() {
        // It accepts the socket, then says nothing: without a bound, the TLS
        // and startup exchanges would wait forever.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let silent = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        let store =
            PostgresStore::new("research", &config("public", &[]), BTreeMap::new()).unwrap();
        for mode in ["disable", "require"] {
            let started = std::time::Instant::now();
            let url = format!(
                "postgres://alice:hunter2@127.0.0.1:{port}/db?connect_timeout=1&sslmode={mode}"
            );
            let attempt = store.connect_to(&url);
            let err = tokio::time::timeout(std::time::Duration::from_secs(10), attempt)
                .await
                .expect("the attempt ends on its own")
                .err()
                .unwrap()
                .to_string();
            assert!(err.contains("no answer within 1s"), "{mode}: {err}");
            assert!(
                started.elapsed() < std::time::Duration::from_secs(5),
                "{mode}"
            );
            assert!(!err.contains("hunter2"), "{err}");
        }
        silent.abort();
    }

    #[test]
    fn malformed_connection_strings_are_not_quoted() {
        let port = connection_config("DB_URL", "postgres://alice:hunter2@db:nope/app").unwrap_err();
        assert!(port.contains("invalid value for option `port`"), "{port}");
        for url in [
            "host=db password=hunter2 hunter3=x",
            "host=db password='hunter2",
            "postgres://alice:hunter2@db/app?hunter3=x",
        ] {
            let err = connection_config("DB_URL", url).unwrap_err();
            assert!(err.contains("`DB_URL` does not hold a valid"), "{err}");
            assert!(!err.contains("hunter"), "{url}: {err}");
        }
    }

    #[tokio::test]
    async fn unknown_types_are_errors_not_queries() {
        let store = PostgresStore::new(
            "research",
            &config("public", &[]),
            pairs(&[("Paper", "id")]),
        )
        .unwrap();
        // No connection string is set: these fail before any I/O.
        let err = store.get("Nope", "x").await.unwrap_err().to_string();
        assert!(
            err.contains("`Nope` is not an entity type of sub-ontology `research`"),
            "{err}"
        );
        assert!(store.list("Nope", &ListQuery::default()).await.is_err());
    }

    #[tokio::test]
    async fn types_stored_elsewhere_need_no_table() {
        let stored = BTreeSet::from(["Paper".to_string()]);
        let keys = pairs(&[("Paper", "id"), ("Person", "id")]);
        let store = PostgresStore::for_types(
            "research",
            &config("public", &[]),
            keys.clone(),
            &stored,
            &BTreeMap::new(),
        )
        .unwrap();
        // No connection string is set: these answer without any I/O.
        assert_eq!(store.get("Person", "vaswani").await.unwrap(), None);
        assert!(store
            .list("Person", &ListQuery::default())
            .await
            .unwrap()
            .is_empty());
        let err = PostgresStore::for_types(
            "research",
            &config("public", &[("Person", "people")]),
            keys,
            &stored,
            &BTreeMap::new(),
        )
        .err()
        .unwrap()
        .to_string();
        assert!(err.contains("stores no fields of it"), "{err}");
    }

    #[tokio::test]
    async fn an_unset_url_env_is_named_in_the_error() {
        let mut config = config("public", &[]);
        config.url_env = "SCRIPTORIUM_ONTOLOGY_TEST_UNSET_URL".into();
        let store = PostgresStore::new("research", &config, pairs(&[("Paper", "id")])).unwrap();
        for err in [
            store.ready().await.unwrap_err().to_string(),
            store.get("Paper", "x").await.unwrap_err().to_string(),
        ] {
            assert!(
                err.contains(
                    "environment variable `SCRIPTORIUM_ONTOLOGY_TEST_UNSET_URL` is not set"
                ),
                "{err}"
            );
        }
    }

    #[tokio::test]
    async fn connection_failures_do_not_reveal_the_connection_string() {
        let store =
            PostgresStore::new("research", &config("public", &[]), BTreeMap::new()).unwrap();
        let err = store
            .connect_to("postgres://alice:hunter2@127.0.0.1:1/app?connect_timeout=5")
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("connecting with `DB_URL`"), "{err}");
        assert!(!err.contains("hunter2") && !err.contains("alice"), "{err}");
    }

    // Integration tests: they run against the database named by
    // `SCRIPTORIUM_ONTOLOGY_TEST_POSTGRES_URL` and skip (with a note) when it
    // is unset. Each test works in a schema of its own.

    const TEST_URL_ENV: &str = "SCRIPTORIUM_ONTOLOGY_TEST_POSTGRES_URL";

    /// Notes that a Postgres integration test does not run here, and why;
    /// under `SCRIPTORIUM_ONTOLOGY_TEST_STRICT`, as CI sets it, fails it
    /// instead.
    fn skip(why: &str) {
        let strict = std::env::var_os("SCRIPTORIUM_ONTOLOGY_TEST_STRICT")
            .is_some_and(|v| !v.is_empty() && v != "0");
        assert!(
            !strict,
            "a Postgres integration test cannot run: {why} (SCRIPTORIUM_ONTOLOGY_TEST_STRICT is set, so nothing may skip)"
        );
        eprintln!("note: skipping a Postgres integration test: {why}");
    }

    struct TestDb {
        client: Client,
        schema: String,
    }

    /// A name no other run uses: `base` plus this process and a counter, so
    /// a test only ever creates, and drops, what it made itself.
    fn unique(base: &str) -> String {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() % 1_000_000_000);
        format!("{base}_{}_{at}_{n}", std::process::id())
    }

    impl TestDb {
        /// A new schema named after `base` and unique to this run, set as
        /// the setup connection's search path.
        async fn open(base: &str) -> Option<Self> {
            let Ok(url) = std::env::var(TEST_URL_ENV) else {
                skip(&format!("set {TEST_URL_ENV}"));
                return None;
            };
            let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
                .await
                .unwrap_or_else(|e| panic!("connecting to the test database: {}", error_chain(&e)));
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let db = Self {
                client,
                schema: unique(base),
            };
            let quoted = quote_ident(&db.schema);
            db.exec(&format!(
                "CREATE SCHEMA {quoted}; SET search_path TO {quoted}"
            ))
            .await;
            Some(db)
        }

        async fn exec(&self, sql: &str) {
            if let Err(e) = self.client.batch_execute(sql).await {
                panic!("{sql}: {}", error_chain(&e));
            }
        }

        /// A store over `types`: (type, key column, stored field columns).
        fn store(&self, tables: &[(&str, &str)], types: &[(&str, &str, &[&str])]) -> PostgresStore {
            let config = PostgresConfig {
                url_env: TEST_URL_ENV.into(),
                schema: self.schema.clone(),
                tables: pairs(tables),
            };
            let keys: BTreeMap<String, String> = types
                .iter()
                .map(|(t, k, _)| ((*t).to_string(), (*k).to_string()))
                .collect();
            let fields: BTreeMap<String, Vec<Column>> = types
                .iter()
                .map(|(t, _, f)| {
                    let columns = f
                        .iter()
                        .map(|name| Column {
                            name: (*name).to_string(),
                            text: None,
                        })
                        .collect();
                    ((*t).to_string(), columns)
                })
                .collect();
            let stored = keys.keys().cloned().collect();
            PostgresStore::for_types("test", &config, keys, &stored, &fields).unwrap()
        }

        async fn finish(self) {
            self.exec(&format!(
                "DROP SCHEMA {} CASCADE",
                quote_ident(&self.schema)
            ))
            .await;
        }
    }

    fn keys_of(records: &[Arc<Record>], key_field: &str) -> Vec<Json> {
        records.iter().map(|r| r[key_field].clone()).collect()
    }

    fn limited(limit: usize) -> ListQuery {
        ListQuery {
            references: None,
            limit: Some(limit),
        }
    }

    fn referencing(field: &str, key: &str) -> ListQuery {
        ListQuery {
            references: Some((field.into(), key.into())),
            limit: None,
        }
    }

    #[tokio::test]
    async fn rows_are_read_by_key_and_listed_in_key_order() {
        let Some(db) = TestDb::open(r#"Ontology Test "Rows""#).await else {
            return;
        };
        db.exec(
            r#"
            CREATE TABLE paper (id text PRIMARY KEY, title text NOT NULL, year integer, t text);
            INSERT INTO paper VALUES ('moe', 'MoE', 2017, 'shadow'), ('attention', 'Attention', 2017, NULL),
                                     ('bert', 'BERT', 2018, NULL);
            CREATE TABLE item (id integer PRIMARY KEY, label text);
            INSERT INTO item VALUES (9, 'nine'), (100, 'hundred'), (10, 'ten');
            CREATE TABLE "Odd ""Table""" ("paperId" text PRIMARY KEY, "camelCase" text);
            INSERT INTO "Odd ""Table""" VALUES ('k', 'v');
            "#,
        )
        .await;
        let store = db.store(
            &[("Odd", r#"Odd "Table""#)],
            &[
                ("Paper", "id", &["title", "year", "t"]),
                ("Item", "id", &["label"]),
                ("Odd", "paperId", &["camelCase"]),
            ],
        );
        store.ready().await.unwrap();

        let attention = store.get("Paper", "attention").await.unwrap().unwrap();
        assert_eq!(
            Json::Object((*attention).clone()),
            json!({"id": "attention", "title": "Attention", "year": 2017, "t": null})
        );
        let moe = store.get("Paper", "moe").await.unwrap().unwrap();
        assert_eq!(
            moe["t"],
            json!("shadow"),
            "a column named `t` is just a field"
        );
        assert!(store.get("Paper", "missing").await.unwrap().is_none());
        assert!(store.get("Paper", "nul\0key").await.unwrap().is_none());

        let all = store.list("Paper", &ListQuery::default()).await.unwrap();
        assert_eq!(
            keys_of(&all, "id"),
            [json!("attention"), json!("bert"), json!("moe")]
        );
        let two = store.list("Paper", &limited(2)).await.unwrap();
        assert_eq!(keys_of(&two, "id"), [json!("attention"), json!("bert")]);
        assert!(store.list("Paper", &limited(0)).await.unwrap().is_empty());

        // Integer keys: looked up and ordered as text.
        // Keys are read as text: exact, whatever the column's type.
        let ten = store.get("Item", "10").await.unwrap().unwrap();
        assert_eq!(
            Json::Object((*ten).clone()),
            json!({"id": "10", "label": "ten"})
        );
        assert!(
            store.get("Item", "ten").await.unwrap().is_none(),
            "not an error"
        );
        let items = store.list("Item", &ListQuery::default()).await.unwrap();
        assert_eq!(
            keys_of(&items, "id"),
            [json!("10"), json!("100"), json!("9")]
        );

        let odd = store.get("Odd", "k").await.unwrap().unwrap();
        assert_eq!(
            Json::Object((*odd).clone()),
            json!({"paperId": "k", "camelCase": "v"})
        );

        let err = store.get("Nope", "x").await.unwrap_err().to_string();
        assert!(err.contains("not an entity type"), "{err}");
        db.finish().await;
    }

    #[tokio::test]
    async fn references_match_scalars_and_arrays_as_text() {
        let Some(db) = TestDb::open("ontology_test_references").await else {
            return;
        };
        db.exec(
            r#"
            CREATE TABLE link (id text PRIMARY KEY, author text, authors text[], editor integer,
                               editors integer[], flag boolean, meta jsonb);
            INSERT INTO link VALUES
              ('l1', 'vaswani', '{vaswani,shazeer}', 7, '{7,8}', true, '{"k": "vaswani"}'),
              ('l2', 'shazeer', '{shazeer}', 8, '{8}', false, '["vaswani", 7]'),
              ('l3', NULL, '{}', NULL, NULL, NULL, '"vaswani"'),
              ('l0', 'vaswani', '{vaswani}', 7, '{7}', true, NULL);
            "#,
        )
        .await;
        let store = db.store(
            &[],
            &[(
                "Link",
                "id",
                &["author", "authors", "editor", "editors", "flag", "meta"],
            )],
        );
        let ids = |records: Vec<Arc<Record>>| keys_of(&records, "id");
        let refs = |field: &str, key: &str| {
            let query = referencing(field, key);
            let store = &store;
            async move { ids(store.list("Link", &query).await.unwrap()) }
        };
        assert_eq!(
            refs("author", "vaswani").await,
            [json!("l0"), json!("l1")],
            "text"
        );
        assert_eq!(
            refs("authors", "shazeer").await,
            [json!("l1"), json!("l2")],
            "text[]"
        );
        assert_eq!(
            refs("editor", "7").await,
            [json!("l0"), json!("l1")],
            "integer"
        );
        assert_eq!(
            refs("editors", "8").await,
            [json!("l1"), json!("l2")],
            "integer[]"
        );
        assert!(refs("editor", "07").await.is_empty(), "compared as text");
        assert!(refs("nope", "vaswani").await.is_empty(), "no such column");
        let first = ListQuery {
            limit: Some(1),
            ..referencing("authors", "vaswani")
        };
        assert_eq!(
            ids(store.list("Link", &first).await.unwrap()),
            [json!("l0")]
        );

        // The SQL filter agrees with `ListQuery::matches` everywhere.
        let all = store.list("Link", &ListQuery::default()).await.unwrap();
        for field in [
            "author", "authors", "editor", "editors", "flag", "meta", "nope",
        ] {
            for key in ["vaswani", "shazeer", "7", "8", "07", "true", "nobody"] {
                let query = referencing(field, key);
                let expected: Vec<Arc<Record>> =
                    all.iter().filter(|r| query.matches(r)).cloned().collect();
                assert_eq!(refs(field, key).await, ids(expected), "{field} = {key}");
            }
        }
        db.finish().await;
    }

    #[test]
    fn identifiers_postgres_would_cut_are_refused() {
        // Two tables that share their first 63 bytes would read one table.
        let long = format!("{}_a", "t".repeat(63));
        let err = PostgresStore::new(
            "web",
            &config("public", &[("Page", long.as_str())]),
            pairs(&[("Page", "id")]),
        )
        .err()
        .unwrap()
        .to_string();
        assert!(err.contains("cuts identifiers to 63"), "{err}");
        // So would two columns.
        let column = format!("{}_x", "c".repeat(63));
        let fields = BTreeMap::from([(
            "Page".to_string(),
            vec![Column {
                name: column,
                text: None,
            }],
        )]);
        let err = PostgresStore::for_types(
            "web",
            &config("public", &[]),
            pairs(&[("Page", "id")]),
            &BTreeSet::from(["Page".to_string()]),
            &fields,
        )
        .err()
        .unwrap()
        .to_string();
        assert!(err.contains("the column `Page."), "{err}");
        assert!(PostgresStore::new(
            "web",
            &config("public", &[("Page", &"t".repeat(63))]),
            pairs(&[("Page", "id")]),
        )
        .is_ok());
    }

    #[test]
    fn types_reading_one_table_are_rejected() {
        // Both default to `http_request`.
        let err = PostgresStore::new(
            "web",
            &config("public", &[]),
            pairs(&[("HTTPRequest", "id"), ("HttpRequest", "id")]),
        )
        .err()
        .unwrap()
        .to_string();
        assert!(
            err.contains(
                "`HTTPRequest` and `HttpRequest` would both read \"public\".\"http_request\""
            ),
            "{err}"
        );
        // Mapping one elsewhere resolves it.
        assert!(PostgresStore::new(
            "web",
            &config("public", &[("HttpRequest", "http_requests")]),
            pairs(&[("HTTPRequest", "id"), ("HttpRequest", "id")]),
        )
        .is_ok());
    }

    #[tokio::test]
    async fn keys_are_unique_and_present() {
        let Some(db) = TestDb::open("ontology_test_keys").await else {
            return;
        };
        db.exec(
            "
            CREATE TABLE loose (id text, title text);
            CREATE TABLE twice_src (id text, title text);
            INSERT INTO twice_src VALUES ('a', 'one'), ('a', 'two'), (NULL, 'three');
            CREATE VIEW twice AS SELECT * FROM twice_src WHERE id IS NOT NULL;
            CREATE VIEW keyless AS SELECT * FROM twice_src WHERE id IS NULL;
            ",
        )
        .await;
        // A table must declare its key, and a view is refused…
        let err = db
            .store(&[], &[("Loose", "id", &["title"])])
            .ready()
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("not declared unique and NOT NULL"), "{err}");
        let twice = db.store(&[], &[("Twice", "id", &["title"])]);
        let err = twice.ready().await.unwrap_err().to_string();
        assert!(err.contains("not tables"), "{err}");
        // …and rows are still checked as they are read.
        let err = twice.get("Twice", "a").await.unwrap_err().to_string();
        assert!(err.contains("more than one row has key `a`"), "{err}");
        let err = twice
            .list("Twice", &ListQuery::default())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("more than one row has key `a`"), "{err}");
        let err = db
            .store(&[], &[("Keyless", "id", &["title"])])
            .list("Keyless", &ListQuery::default())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("a row has no `id`"), "{err}");
        db.finish().await;
    }

    #[tokio::test]
    async fn an_invalid_unique_index_guarantees_nothing() {
        let Some(db) = TestDb::open("ontology_test_invalid_index").await else {
            return;
        };
        db.exec("CREATE TABLE dup (id text NOT NULL, title text); INSERT INTO dup VALUES ('a', 'x'), ('a', 'y');")
            .await;
        // Fails on the duplicate and leaves an invalid unique index behind.
        assert!(db
            .client
            .batch_execute("CREATE UNIQUE INDEX CONCURRENTLY dup_id ON dup (id)")
            .await
            .is_err());
        let err = db
            .store(&[], &[("Dup", "id", &["title"])])
            .ready()
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("not declared unique and NOT NULL"), "{err}");
        db.finish().await;
    }

    #[tokio::test]
    async fn ready_reads_the_rows_not_just_the_catalog() {
        let Some(db) = TestDb::open("ontology_test_perm").await else {
            return;
        };
        let role = unique("ontology_test_meta");
        db.exec(&format!(
            "CREATE ROLE {role} LOGIN PASSWORD 'meta'; \
             GRANT USAGE ON SCHEMA {} TO {role}; \
             CREATE TABLE secret (id text PRIMARY KEY);",
            quote_ident(&db.schema)
        ))
        .await;
        // The same server and database, as a role that can see the catalogs
        // but not the rows.
        let admin: Config = std::env::var(TEST_URL_ENV).unwrap().parse().unwrap();
        let Host::Tcp(host) = &admin.get_hosts()[0] else {
            panic!("the test database is reached over TCP");
        };
        let restricted = format!(
            "host={host} port={} dbname={} user={role} password=meta",
            admin.get_ports().first().copied().unwrap_or(5432),
            admin.get_dbname().unwrap_or("postgres"),
        );
        let store = db.store(&[], &[("Secret", "id", &[])]);
        let client = store.connect_to(&restricted).await.unwrap().client;
        let err = store.check_tables(&client).await.unwrap_err().to_string();
        assert!(
            err.contains("cannot read") && err.contains("permission denied"),
            "{err}"
        );
        db.exec(&format!("GRANT SELECT ON secret TO {role}")).await;
        store.check_tables(&client).await.unwrap();
        drop(client);
        db.exec(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
            .await;
        db.finish().await;
    }

    #[tokio::test]
    async fn column_grants_on_the_declared_fields_suffice() {
        let Some(db) = TestDb::open("ontology_test_columns").await else {
            return;
        };
        let role = unique("ontology_test_columns");
        db.exec(&format!(
            "CREATE ROLE {role} LOGIN PASSWORD 'cols'; \
             GRANT USAGE ON SCHEMA {} TO {role}; \
             CREATE TABLE note (id text PRIMARY KEY, title text, private_note text); \
             INSERT INTO note VALUES ('n1', 'Public', 'secret'); \
             GRANT SELECT (id, title) ON note TO {role};",
            quote_ident(&db.schema)
        ))
        .await;
        let admin: Config = std::env::var(TEST_URL_ENV).unwrap().parse().unwrap();
        let Host::Tcp(host) = &admin.get_hosts()[0] else {
            panic!("the test database is reached over TCP");
        };
        let restricted = format!(
            "host={host} port={} dbname={} user={role} password=cols",
            admin.get_ports().first().copied().unwrap_or(5432),
            admin.get_dbname().unwrap_or("postgres"),
        );
        // The schema declares `title` only: its grant is enough.
        let store = db.store(&[], &[("Note", "id", &["title"])]);
        let client = store.connect_to(&restricted).await.unwrap().client;
        store.check_tables(&client).await.unwrap();
        let row = client
            .query_typed(&store.tables["Note"].get, &[(&"n1", Type::TEXT)])
            .await
            .unwrap();
        assert_eq!(
            store.record(&store.tables["Note"], &row[0]).unwrap(),
            json!({"id": "n1", "title": "Public"})
                .as_object()
                .unwrap()
                .clone()
        );
        // Declaring a column the role may not read fails at startup.
        let wider = db.store(&[], &[("Note", "id", &["title", "private_note"])]);
        let err = wider.check_tables(&client).await.unwrap_err().to_string();
        assert!(err.contains("permission denied"), "{err}");
        drop(client);
        db.exec(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
            .await;
        db.finish().await;
    }

    #[tokio::test]
    async fn wide_numeric_keys_keep_every_digit_through_graphql() {
        use crate::serve::schema::{build_schema, execute};
        const WIDE: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Wide @key(fields: "id") { id: ID! title: String cites: [Wide!]! score: Float alt: ID alts: [ID!] }
type Query { wide(id: ID!): Wide wides(first: Int): [Wide!]! }
"#;
        let Some(db) = TestDb::open("ontology_test_wide").await else {
            return;
        };
        db.exec(
            "CREATE TABLE wide (id numeric PRIMARY KEY, title text, cites numeric[], score numeric, alt numeric, alts numeric[]);
             INSERT INTO wide VALUES
               (18446744073709551617, 'beyond u64', '{18446744073709551618}', 18446744073709551616, 18446744073709551619, '{18446744073709551620}'),
               (18446744073709551618, 'next', '{}', NULL, NULL, NULL);",
        )
        .await;
        let model = crate::model::SubOntologyModel::parse(WIDE).unwrap();
        let storage = crate::manifest::StorageConfig::Postgres(PostgresConfig {
            url_env: TEST_URL_ENV.into(),
            schema: db.schema.clone(),
            tables: BTreeMap::new(),
        });
        let sub = crate::registry::SubOntology {
            name: "wide".into(),
            dir: "ontology/wide".into(),
            description: None,
            owners: Vec::new(),
            sdl: WIDE.into(),
            source: crate::registry::SubOntologySource::Local(storage.clone()),
        };
        let repo = tempfile::tempdir().unwrap();
        crate::snapshot::test_support::commit_files(repo.path(), &[("README", "x")], "init");
        let snapshot = crate::snapshot::GitSnapshot::open(repo.path(), "HEAD").unwrap();
        let store = super::super::open_store(&sub, &storage, Arc::new(snapshot), &model).unwrap();
        store.ready().await.unwrap();
        let schema = build_schema("wide", WIDE, store).unwrap();
        let response = execute(
            &schema,
            "{ wides { id cites { id } score alt alts } wide(id: \"18446744073709551618\") { title } }"
                .into(),
        )
        .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        assert_eq!(
            response.data.into_json().unwrap(),
            json!({
                "wides": [
                    {"id": "18446744073709551617", "cites": [{"id": "18446744073709551618"}], "score": 18_446_744_073_709_551_616.0, "alt": "18446744073709551619", "alts": ["18446744073709551620"]},
                    {"id": "18446744073709551618", "cites": [], "score": null, "alt": null, "alts": null}
                ],
                "wide": {"title": "next"}
            })
        );
        db.finish().await;
    }

    #[tokio::test]
    async fn ready_names_missing_tables_and_key_columns() {
        let Some(db) = TestDb::open("ontology_test_ready").await else {
            return;
        };
        db.exec("CREATE TABLE paper (id text PRIMARY KEY); CREATE TABLE no_key (name text);")
            .await;
        db.store(&[], &[("Paper", "id", &[])])
            .ready()
            .await
            .unwrap();
        let err = db
            .store(
                &[("Mapped", "elsewhere")],
                &[
                    ("Paper", "id", &[]),
                    ("NoKey", "id", &[]),
                    ("Missing", "id", &[]),
                    ("Mapped", "id", &[]),
                ],
            )
            .ready()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(&format!(
                r#"missing tables: "{s}"."elsewhere" (`Mapped`), "{s}"."missing" (`Missing`)"#,
                s = db.schema
            )),
            "{err}"
        );
        assert!(
            err.contains(&format!(
                r#"missing key columns: "{}"."no_key"."id" (`NoKey`)"#,
                db.schema
            )),
            "{err}"
        );
        assert!(!err.contains(r#""paper""#), "{err}");
        db.finish().await;
    }

    #[tokio::test]
    async fn a_read_without_an_answer_fails_and_the_next_reconnects() {
        let Some(db) = TestDb::open("ontology_test_stall").await else {
            return;
        };
        db.exec("CREATE TABLE paper (id text PRIMARY KEY); INSERT INTO paper VALUES ('a');")
            .await;
        let mut store = db.store(&[], &[("Paper", "id", &[])]);
        store.query_timeout = std::time::Duration::from_secs(1);
        assert!(store.get("Paper", "a").await.unwrap().is_some());
        let first = store.client().await.unwrap();
        let driver = store.client.lock().await.as_ref().unwrap().driver.clone();

        // Another session holds the table, so reads wait for its lock: the
        // server is up, but this read gets no answer.
        let url = std::env::var(TEST_URL_ENV).unwrap();
        let (locker, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        locker
            .batch_execute(&format!(
                "BEGIN; LOCK TABLE {}.paper IN ACCESS EXCLUSIVE MODE",
                quote_ident(&db.schema)
            ))
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let err = tokio::time::timeout(std::time::Duration::from_secs(10), store.get("Paper", "a"))
            .await
            .expect("the read ends on its own")
            .unwrap_err()
            .to_string();
        assert!(err.contains("no answer within 1s"), "{err}");
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        // Its socket is closed while the lock is still held: left running,
        // the task would wait for that answer as long as the server stalls.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !driver.is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "the stalled connection's task still runs"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        locker.batch_execute("ROLLBACK").await.unwrap();

        // The stalled connection was dropped: the next read opens another.
        assert!(store.get("Paper", "a").await.unwrap().is_some());
        assert!(!Arc::ptr_eq(&first, &store.client().await.unwrap()));
        db.finish().await;
    }

    #[tokio::test]
    async fn a_stalled_host_gives_way_to_the_next() {
        let Some(db) = TestDb::open("ontology_test_failover").await else {
            return;
        };
        db.exec("CREATE TABLE paper (id text PRIMARY KEY); INSERT INTO paper VALUES ('a');")
            .await;
        let store = db.store(&[], &[("Paper", "id", &[])]);
        let mut real =
            connection_config(TEST_URL_ENV, &std::env::var(TEST_URL_ENV).unwrap()).unwrap();
        real.connect_timeout(std::time::Duration::from_secs(1));
        let (silent, task) = silent_server().await;
        // The silent host first, then the test database wherever it is (a
        // host, a `hostaddr`, or a socket path), with its own options.
        let mut both = vec![Destination {
            host: Some(Host::Tcp("127.0.0.1".into())),
            hostaddr: None,
            port: silent,
        }];
        both.extend(destinations(&real).unwrap());
        let started = std::time::Instant::now();
        let session = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            store.try_each(&real, both),
        )
        .await
        .expect("the attempt ends on its own")
        .unwrap_or_else(|e| panic!("{e}"));
        assert!(started.elapsed() >= std::time::Duration::from_millis(900));
        let rows = session.client.query("SELECT 1", &[]).await.unwrap();
        assert_eq!(rows.len(), 1);
        task.abort();
        db.finish().await;
    }

    #[tokio::test]
    async fn a_closed_connection_is_replaced() {
        let Some(db) = TestDb::open("ontology_test_reconnect").await else {
            return;
        };
        db.exec("CREATE TABLE paper (id text PRIMARY KEY); INSERT INTO paper VALUES ('a');")
            .await;
        let store = db.store(&[], &[("Paper", "id", &[])]);
        assert!(store.get("Paper", "a").await.unwrap().is_some());
        let first = store.client().await.unwrap();

        // End the store's session from the server side: its last query names
        // this test's schema.
        let terminated = db
            .client
            .query(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                 WHERE pid <> pg_backend_pid() AND strpos(query, $1) > 0",
                &[&quote_ident(&db.schema)],
            )
            .await
            .unwrap();
        assert_eq!(terminated.len(), 1, "one store session");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !first.is_closed() {
            assert!(
                std::time::Instant::now() < deadline,
                "the session never closed"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        assert!(
            store.get("Paper", "a").await.unwrap().is_some(),
            "reconnected"
        );
        assert!(!Arc::ptr_eq(&first, &store.client().await.unwrap()));
        db.finish().await;
    }

    const RESEARCH: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
directive @inverse(of: String!) on FIELD_DEFINITION
type Paper @key(fields: "id") {
  id: ID!
  title: String!
  year: Int
  authors: [Person!]!
  pages: PageRange
}
type PageRange { first: Int! last: Int! }
extend type Person @key(fields: "id") {
  id: ID!
  papers: [Paper!]! @inverse(of: "authors")
}
type Query {
  paper(id: ID!): Paper
  papers(first: Int): [Paper!]!
}
"#;

    #[tokio::test]
    async fn serves_a_subgraph_end_to_end() {
        use crate::serve::schema::{build_schema, execute};
        let Some(db) = TestDb::open("ontology_test_serve").await else {
            return;
        };
        db.exec(
            r#"
            CREATE TABLE paper (id text PRIMARY KEY, title text NOT NULL, year integer,
                                authors text[] NOT NULL DEFAULT '{}', pages jsonb);
            INSERT INTO paper VALUES
              ('attention', 'Attention Is All You Need', 2017, '{vaswani,shazeer}', '{"first": 1, "last": 11}'),
              ('moe', 'Outrageously Large Neural Networks', 2017, '{shazeer}', NULL);
            "#,
        )
        .await;
        let model = crate::model::SubOntologyModel::parse(RESEARCH).unwrap();
        let config = PostgresConfig {
            url_env: TEST_URL_ENV.into(),
            schema: db.schema.clone(),
            tables: BTreeMap::new(),
        };
        let sub = crate::registry::SubOntology {
            name: "research".into(),
            dir: "ontology/research".into(),
            description: None,
            owners: Vec::new(),
            sdl: RESEARCH.into(),
            source: crate::registry::SubOntologySource::Local(
                crate::manifest::StorageConfig::Postgres(config.clone()),
            ),
        };
        // Through `open_store`, as `serve` opens it: research only extends
        // Person with a derived field, so Person needs no table.
        let repo = tempfile::tempdir().unwrap();
        crate::snapshot::test_support::commit_files(repo.path(), &[("README", "x")], "init");
        let snapshot = crate::snapshot::GitSnapshot::open(repo.path(), "HEAD").unwrap();
        let store = super::super::open_store(
            &sub,
            &crate::manifest::StorageConfig::Postgres(config),
            Arc::new(snapshot),
            &model,
        )
        .unwrap();
        store.ready().await.unwrap();
        let schema = build_schema("research", RESEARCH, store).unwrap();

        let response = execute(
            &schema,
            r#"{ paper(id: "attention") { id title year authors { id } pages { first last } }
                 papers(first: 1) { id } }"#
                .into(),
        )
        .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        assert_eq!(
            response.data.into_json().unwrap(),
            json!({
                "paper": {
                    "id": "attention", "title": "Attention Is All You Need", "year": 2017,
                    "authors": [{"id": "vaswani"}, {"id": "shazeer"}],
                    "pages": {"first": 1, "last": 11}
                },
                "papers": [{"id": "attention"}]
            })
        );

        let response = execute(
            &schema,
            r#"{ _entities(representations: [
                   {__typename: "Person", id: "shazeer"},
                   {__typename: "Paper", id: "moe"},
                   {__typename: "Paper", id: "missing"}
                 ]) {
                   ... on Person { id papers { id } }
                   ... on Paper { title }
                 } }"#
                .into(),
        )
        .await;
        assert_eq!(
            response.data.into_json().unwrap(),
            json!({"_entities": [
                {"id": "shazeer", "papers": [{"id": "attention"}, {"id": "moe"}]},
                {"title": "Outrageously Large Neural Networks"},
                null
            ]})
        );
        assert_eq!(response.errors.len(), 1, "{:?}", response.errors);
        // The schema and the variable naming the database stay out of it.
        assert_eq!(
            response.errors[0].message, "Paper:missing does not exist",
            "{}",
            db.schema
        );
        db.finish().await;
    }
}
