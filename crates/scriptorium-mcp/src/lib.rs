//! Scriptorium MCP server.
//!
//! Exposes the [`scriptorium_core`] engine to Claude Code (and any other
//! MCP-speaking client) over JSON-RPC 2.0 on stdio. The transport is
//! deliberately hand-rolled rather than pulling in the `rmcp` crate, for two
//! reasons:
//!
//! - the stdio framing is tiny (one JSON object per line) and the methods we
//!   need (`initialize`, `notifications/initialized`, `tools/list`,
//!   `tools/call`) fit in a single file;
//! - avoiding the external SDK means the provider trait inside `core` stays
//!   our sole dependency boundary, which makes it easier to keep the server
//!   in lockstep with engine changes.
//!
//! ## Protocol summary
//!
//! ```text
//! → {"jsonrpc":"2.0","id":1,"method":"initialize","params":{...}}
//! ← {"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"...","serverInfo":{...},"capabilities":{"tools":{}}}}
//! → {"jsonrpc":"2.0","method":"notifications/initialized"}
//! → {"jsonrpc":"2.0","id":2,"method":"tools/list"}
//! ← {"jsonrpc":"2.0","id":2,"result":{"tools":[...]}}
//! → {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"...","arguments":{...}}}
//! ← {"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"..."}]}}
//! ```

pub mod server;
pub mod tools;

pub use server::{serve_stdio, ServerContext};
pub use tools::{ToolError, ToolRegistry};
