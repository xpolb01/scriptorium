# Federated ontology (experimental)

> Status: experiment. Formats and commands can change without notice.

This document tells you how the ontology works, how to use it, and how
the tests prove it.

## Summary

- **Git is the backend.** Each schema, store configuration, and git
  record is a file in a git repository. A commit is a version of the
  ontology.
- **A sub-ontology is a GraphQL Federation v2 subgraph.** Composition
  merges all sub-ontologies into one supergraph. Composition stops when
  two sub-ontologies do not agree.
- **Each sub-ontology has one store:** git records, markdown notes,
  Postgres, or DynamoDB. A sub-ontology can also be remote: a different
  server supplies it.
- **The runtime is a real federation.** `scriptorium-ontology serve`
  serves each local sub-ontology as a subgraph. A Federation v2 gateway
  plans each query. The tests use Hive Router, Hive Gateway, and Apollo
  Router.

## Architecture

```mermaid
flowchart LR
    Author([person or agent]) -- "edit and commit" --> Git
    subgraph Git["git repository: the source of truth"]
        direction TB
        Schemas["schema.graphql<br/>subgraph.toml"]
        Records["git records<br/>markdown notes"]
        Pins["pinned remote schemas"]
    end
    Git --> Publish["serve --supergraph<br/>or compose --out"]
    Publish --> Super[/"supergraph.graphql"/]
    Super --> Gateway["federation gateway"]
    Git --> Serve["serve: one subgraph for<br/>each local sub-ontology"]
    Serve --> Stores[("git, markdown,<br/>Postgres, DynamoDB")]
    Client([user or agent]) -- "GraphQL query" --> Gateway
    Gateway -- "root fields and _entities" --> Serve
    Gateway --> Remote["remote subgraph"]
    Remote -. "add-remote and pull pin its schema" .-> Pins
```

Two roads go to a user:

1. **A change.** A person edits files and makes a commit. `check`
   examines the commit. Publication writes the supergraph only when the
   commit composes and each store answers. Then the gateway loads the new
   supergraph.
2. **An answer.** A user sends a query to the gateway. The gateway divides
   the query into fetches. Each subgraph reads its store and answers. The
   gateway merges the answers.

| Component | Function |
|---|---|
| Git repository | Keeps the schemas, the store configurations, the git records, and the remote pins. Each commit is a version. |
| `check` | Examines a commit: registry, composition, schemas, records, and store configurations. |
| `compose` | Merges the sub-ontologies of a commit into a supergraph. |
| `serve` | Serves each local sub-ontology at `<base URL>/<name>/graphql`. With `--supergraph`, it also publishes the supergraph. |
| Store | Holds the entities of one sub-ontology. |
| Remote sub-ontology | A subgraph that a different server supplies. Git keeps a pinned copy of its schema. |
| Gateway | Loads the supergraph, plans each query, and sends the fetches to the subgraphs. |

## Vocabulary

| Ontology term | Federation mechanism |
|---|---|
| Sub-ontology | Subgraph |
| Class with identity | Object type with `@key(fields: "id")`, an entity |
| Property | Scalar or enum field |
| Relation | Field with an entity type. The store keeps the key of the target. |
| Inverse relation | Field with `@inverse(of: "<field>")`. The server calculates it. The store does not keep it. |
| Owner of a class | The sub-ontology that declares `type X @key(...)` |
| Extension of a class | `extend type X @key(...)` in a different sub-ontology |
| Move of a property | `@override(from: "<subgraph>")` |
| Ontology conflict | Composition error |
| Composed ontology | Supergraph |
| Ontology version | Git commit |

## Layout

```text
ontology/
├── ontology.toml          # optional: [serve] configuration
├── core/
│   ├── subgraph.toml      # description, owners, and [storage] or [remote]
│   ├── schema.graphql     # the subgraph schema
│   └── data/Person/vaswani.yaml
└── research/
    └── …
```

- A directory under `ontology/` is a sub-ontology when it contains
  `subgraph.toml`.
- The directory name is the default sub-ontology name. To use a
  different name, set `name` in `subgraph.toml`.
- The name must match `[a-z][a-z0-9_]*`. It is also the path of the
  subgraph: `<base URL>/<name>/graphql`.
- `ontology.toml` can set `[serve] addr` (default `127.0.0.1:4200`) and
  `base_url`, the URL that the gateway uses to get to `serve`.
- In the ontology directory, symlinks, git submodules, and file names that
  are not UTF-8 are errors.

`subgraph.toml` contains a `[storage]` table or a `[remote]` table, not
both:

```toml
description = "Papers, citations, authorship."
owners = ["@xpolb01"]

[storage]
kind = "git"          # or "markdown", "postgres", "dynamodb"
# [remote]
# url = "https://partner.example.com/graphql"
```

`schema.graphql` is a Federation v2 subgraph schema. Each root field must
have one of these two forms:

- **Lookup:** `paper(id: ID!): Paper`. It has one argument, the entity
  key. The result can be null. An `Int` key uses `Int!`.
- **List:** `papers(first: Int): [Paper!]!`. The default limit is 100.
  The maximum is 1000.

At least one sub-ontology must declare a `Query` field. A root field
returns only entities that its sub-ontology owns.

Do not put a description on a type extension. In GraphQL, only
definitions have descriptions. Write a `#` comment instead.

## Stores

| Kind | Options | Entities |
|---|---|---|
| `git` | `data_dir` (default `data`) | One YAML file for each entity: `<data_dir>/<Type>/<key>.yaml` |
| `markdown` | `dir`, `type` or `type_field`, `key`, `body_field`, `links_field` | One `.md` file for each entity. The frontmatter holds the fields. |
| `postgres` | `url_env`, `schema` (default `public`), `[storage.tables]` | One table for each entity type |
| `dynamodb` | `table`, `region`, `endpoint_url_env` | One table for all entity types |

The database stores are features. Build with `--features postgres` or
`--features dynamodb`.

### Git records

- A record holds only the fields that its sub-ontology owns. The file
  name is the key.
- A relation holds the key of its target, for example
  `authors: [vaswani, shazeer]`.
- A key that occurs two times in one record is an error.
- The owner of type X decides which X entities exist. An extension adds
  fields, but it does not make an entity exist.
- When two sub-ontologies both declare `type X`, both own X. Each must
  then store every X.

### Markdown notes

- Each `.md` file is one entity. The search includes subdirectories.
- `key = "stem"` uses the file name. `key = "frontmatter:id"` uses a
  frontmatter field.
- In a relation field, a string that is one `[[wikilink]]` refers to the
  note with that file stem.
- `body_field` gets the body text. `links_field` gets the keys of the
  notes that the body links to.
- `body_field` must name a `String` field, and `links_field` a list of
  entities. Neither can be the key field.
- Frontmatter that starts with a `---` line must end with a `---` line.
- Links in code, raw HTML, Markdown links, and embeds do not count.
- `check` ignores frontmatter that the schema does not declare.
- A symlink among the notes is an error.

### Postgres rows

- Each entity type reads one table. The default table name is the type
  name in snake case.
- Each column name is the same as its field name. The key column is the
  key field of the entity.
- The key column must be `NOT NULL` and unique: a primary key, or a
  column with a valid unique index. `serve` checks this at startup. It
  does not accept views.
- The store reads only the key column and the columns of stored fields.
  Column-level `SELECT` grants on these columns are enough.
- Relations hold target keys in `text`, `text[]`, `integer`, or
  `integer[]` columns. `jsonb` columns hold value objects.
- The store uses TLS for each server that is not local, and it verifies
  the certificate and the host name. It does not accept `sslmode=disable`
  for such a server.
- For a local server, the store uses TLS only with `sslmode=require`.
- Write `sslmode=require`, not `sslmode=verify-full`. Here, `require`
  verifies the certificate and the host name.
- The store tries each host of the connection string, and each address of
  a host name, in turn. Each attempt gets the `connect_timeout` of the
  connection string, or 10 s.
- A query gets 30 s. Then the request fails with a reference, the store
  closes the connection, and the next request connects again.
- Because of these limits, a server or a proxy that stops during an
  answer cannot stop `serve`, at startup or during a request.
- Keep the connection string in the environment variable that `url_env`
  names. Do not commit the connection string.

### DynamoDB items

- The table has one HASH key, `pk` (string), with the value
  `<Type>#<key>`.
- Each item has a `type` attribute and one attribute for each field. Do
  not use the names `pk` and `type` for fields.
- The store gets credentials from the standard AWS chain.
- At startup, `serve` checks the key schema. It also does one `GetItem`
  and one `Scan`.
- Lookups use `GetItem`. Lists and inverse relations scan the table.
- A connection attempt stops after 10 s. Each request, with its retries,
  stops after 30 s.

## Remote sub-ontologies

```sh
scriptorium-ontology add-remote partner https://partner.example.com/graphql \
  --header Authorization=PARTNER_TOKEN
scriptorium-ontology pull --dry-run   # show the changes and write nothing
scriptorium-ontology pull             # refresh each remote
```

- `add-remote` gets the schema of the subgraph (`_service { sdl }`). It
  checks that the schema composes. Then it writes and commits
  `ontology/<name>/`.
- Composition always reads the pinned `schema.graphql`. A remote change
  shows as a git diff before it gets to the gateway.
- `pull` writes all changed schemas or none. It writes nothing when a
  fetch fails or when the new set does not compose.
- `pull` does not write over a remote directory that has changes that are
  not committed.
- `add-remote` and `pull` take a lock, so only one of them runs at a time
  in a clone. With `--no-commit`, they write files but do not commit.
- `--header NAME=ENV` sends the value of `$ENV` as the header `NAME`. The
  git directory keeps only the names, in
  `.git/scriptorium-ontology/remotes.toml`. No commit contains a
  credential.
- `pull` sends the headers only to the URL that you set them for. After
  you change the URL of a remote, set the headers again with
  `pull <name> --header …`.
- A remote URL cannot contain a user, a password, a query, or a fragment.
- Headers go over plain `http` only to a loopback host. The commands do not
  follow redirects, and they read at most 8 MiB from a remote.
- The supergraph contains no headers. Give the gateway its own
  credentials for the remote.

## Commands

| Command | Function |
|---|---|
| `init` | Makes the example ontology (`core`, `research`, `software`, `notes`) and commits it. |
| `list` | Shows the sub-ontologies at a revision. |
| `check` | Examines a revision. It stops with a non-zero exit code when it finds an error. |
| `compose` | Writes the supergraph, to a file with `--out`, or the client schema with `--api`. |
| `serve` | Serves the local sub-ontologies. `--supergraph <file>` also publishes the supergraph. |
| `add-remote <name> <url>` | Pins the schema of a remote subgraph and commits it. |
| `pull [<name>…]` | Refreshes the pinned remote schemas. |

Each command reads a git revision: `--rev`, default `HEAD`. `--worktree`
reads files that are not committed, for `list`, `check`, and `compose` to
standard output only. `serve` and `compose --out` read only commits.
Without `-C <dir>`, a command uses the git repository that contains the
current directory.

Each subgraph answers `_service { sdl }` with its committed
`schema.graphql`, with no change. Subgraphs do not answer introspection
(`__schema`, `__type`). Use the gateway for introspection.

### Publication rules

`compose --out` and `serve --supergraph` publish a supergraph only when
all of these conditions are true:

1. Each sub-ontology loads and composes, and `serve` can serve it.
2. Each store answers. The database is available and its tables are
   correct.
3. Each local subgraph at the base URL serves the commit that you publish.
4. Each remote serves the schema that git pins.
5. Each subgraph URL is an absolute `http` or `https` URL that a gateway
   can get to.

`--offline` skips conditions 3 and 4. With `compose --out`, it also skips
condition 2. `serve` always does condition 2.

When nothing listens at the base URL, `compose --out` writes a note and
publishes. Start `serve` at the same commit before a gateway uses the
file.

- Only one publisher at a time writes to a supergraph file. A lock file,
  `.<name>.lock`, stops a second publisher.
- The new file replaces the old file in one step. When the path is a
  symlink, publication replaces the target. The new file keeps the
  permissions, the owner, and the group of the old file.
- The new file does not keep the ACLs or the extended attributes of the
  old file. Give the gateway access through directory permissions or
  default ACLs.
- When a store fails during a query, the client gets a general error with
  a reference. The `serve` log contains the details under that reference.

### Run a federated query

```sh
scriptorium-ontology -C ~/onto init          # example: core, research, software, notes
scriptorium-ontology -C ~/onto serve --supergraph /tmp/supergraph.graphql
npx @graphql-hive/gateway supergraph /tmp/supergraph.graphql
curl -s localhost:4000/graphql -H 'content-type: application/json' \
  -d '{"query":"{ paper(id: \"bert\") { title authors { name } cites { title implementations { url } } } }"}'
```

`research` answers `paper` and `cites`. `core` answers `name`. `software`
answers `implementations`. The gateway plans the `_entities` calls.

## Checks

`check` does these steps in this sequence:

1. **Registry.** Each `subgraph.toml` parses. The names are correct and
   unique. Each sub-ontology is local or remote. `schema.graphql` exists.
2. **Composition.** The sub-ontologies compose, and the supergraph has a
   `Query` field. A description on a type extension gives a warning,
   `ontology.schema.extension_description`.
3. **Servability.** `serve` can serve each local sub-ontology. Each entity
   has one key field of type `ID`, `String`, or `Int`. The root fields are
   lookups and lists. The schema uses no `@requires`, `@provides`,
   `@context`, `@fromContext`, or `@interfaceObject`.
4. **Records.** Git records and markdown notes agree with their schema.
   Each record holds only the fields that its sub-ontology owns.
5. **Existence.** Each relation refers to an entity that exists
   (`ontology.record.dangling_ref`). Each extension record refers to an
   entity that exists (`ontology.record.orphan`). An extension with a
   non-null stored field has a record for each entity
   (`ontology.record.missing`).
   For a database owner or a remote owner, these are warnings.
6. **One resolver for each field.** Only one local sub-ontology resolves
   each field that is not a key (`ontology.schema.shared_field`). A field
   that moves with `@override` must keep the same values
   (`ontology.record.override_gap`).
7. **Storage.** The configuration of each database store is correct. This
   step does not connect to the database.

## Limits

- **Read-only.** `serve` has no mutations. To change the ontology, edit
  the files and make a commit.
- **One commit for each server.** `serve` reads one commit. After a commit,
  start `serve` again.
- **No authentication.** `serve` binds to loopback by default. Put an
  authentication layer in front of it before you expose it.
- **Request limits.** A request has a maximum depth of 32, 5,000 fields,
  1,000 items in a list, and 10,000 `_entities` representations. An
  answer has a maximum of 100,000 list items, 250,000 field resolutions,
  and 32 MiB. There is no rate limit.
- **Publication checks the remote pins.** `serve` does not check them
  again. A remote can change its schema after publication. When the
  schema must stay the same, pin a URL that has a version.
- **Schema subset.** `serve` resolves object types, enums, custom scalars,
  one list level, and one key field for each entity.
- **Inverse relations do not paginate.** More than 1000 matches cause a
  query error.
- **Database stores scan.** Integer-key lookups in Postgres, and lists and
  inverse relations in DynamoDB, scan the table. Keep database
  sub-ontologies small.

## Install

The ontology has its own Cargo workspace and binary,
`scriptorium-ontology`. It is not in the main workspace.
`graphql-composition` turns on the `preserve_order` feature of
`serde_json`. In a shared workspace, this feature changes the JSON key
order of `scriptorium-core`, and also its page hashes.

```sh
cargo install --path crates/scriptorium-ontology                               # git and markdown stores
cargo install --path crates/scriptorium-ontology --features postgres,dynamodb  # all stores
```

The build must use Rust 1.94.1 or newer.

## Tests

| Suite | What it proves | Conditions |
|---|---|---|
| Unit tests (`src/`) | Each module. The Postgres tests use a real database. | `SCRIPTORIUM_ONTOLOGY_TEST_POSTGRES_URL` for the Postgres tests |
| `tests/cli.rs` | Each command, as a process | None |
| `tests/dynamodb.rs` | The DynamoDB store | `--features dynamodb`, `SCRIPTORIUM_ONTOLOGY_TEST_DYNAMODB_ENDPOINT` |
| `tests/federation.rs` | The subgraphs answer the fetches of a query plan | None |
| `tests/gateway.rs` | A real gateway plans queries over the example and special cases | A gateway |
| `tests/gateway_stores.rs` | One dataset gives the same answers from each store | A gateway, and the databases for their parts |
| `tests/e2e.rs` | The claims of this document, through the binary, the stores, and a gateway | A gateway, the databases, and Rover |

| Variable | Value |
|---|---|
| `SCRIPTORIUM_ONTOLOGY_TEST_GATEWAY` | The path of a Hive Router (`hive_router`), Hive Gateway (`hive-gateway`), or Apollo Router (`router`) binary |
| `SCRIPTORIUM_ONTOLOGY_TEST_GATEWAY_KIND` | `hive-router`, `hive-gateway`, or `apollo-router`, when the binary name does not tell |
| `SCRIPTORIUM_ONTOLOGY_TEST_POSTGRES_URL` | A Postgres database that the tests can change |
| `SCRIPTORIUM_ONTOLOGY_TEST_POSTGRES_UNTRUSTED_TLS_URL` | A Postgres server with a self-signed certificate |
| `SCRIPTORIUM_ONTOLOGY_TEST_DYNAMODB_ENDPOINT` | A DynamoDB Local URL |
| `SCRIPTORIUM_ONTOLOGY_TEST_ROVER` | The path of a `rover` binary |
| `APOLLO_ELV2_LICENSE=accept` | You accept the Elastic License 2.0 of the Apollo composition plugin. The test never accepts it for you. |
| `SCRIPTORIUM_ONTOLOGY_TEST_STRICT=1` | A test that cannot run fails. Without this variable, the test writes a note and passes. |

To get the gateways:

```sh
cargo install --locked hive-router --root target/tools          # Hive Router
npm install --prefix target/tools @graphql-hive/gateway          # Hive Gateway
# Apollo Router and Rover: release binaries from github.com/apollographql
```

To run all suites, and make sure that no test skips:

```sh
SCRIPTORIUM_ONTOLOGY_TEST_STRICT=1 \
SCRIPTORIUM_ONTOLOGY_TEST_GATEWAY=$PWD/target/tools/bin/hive_router \
SCRIPTORIUM_ONTOLOGY_TEST_POSTGRES_URL=postgres://postgres:secret@127.0.0.1:5432/postgres \
SCRIPTORIUM_ONTOLOGY_TEST_POSTGRES_UNTRUSTED_TLS_URL='postgres://postgres:secret@localhost:5433/postgres?sslmode=require' \
SCRIPTORIUM_ONTOLOGY_TEST_DYNAMODB_ENDPOINT=http://127.0.0.1:8000 \
SCRIPTORIUM_ONTOLOGY_TEST_ROVER=$PWD/target/tools/rover APOLLO_ELV2_LICENSE=accept \
  cargo test --manifest-path crates/scriptorium-ontology/Cargo.toml --features postgres,dynamodb
```

Run the suites one time for each gateway. Without the database features,
the store tests of `tests/e2e.rs` are not in the build.

The tests turn off the anonymous usage reports of Apollo Router and Rover.

Each test in `tests/e2e.rs` proves one claim:

| Claim | Test |
|---|---|
| The quickstart gives the answer that this document shows. The test reads the query from this document. | `the_documented_quickstart_answers_as_documented` |
| Each commit is a version that you can serve. `serve` never serves files that are not committed. | `every_commit_is_a_version_that_can_be_served` |
| A commit that does not compose never gets to the gateway. | `a_commit_that_does_not_compose_never_reaches_the_gateway` |
| A schema change on a remote gets to clients only through `pull`. | `a_remote_schema_change_reaches_clients_only_through_pull` |
| One gateway joins git, markdown, Postgres, DynamoDB, and a second repository. A move of data to a different store changes no answer. No commit contains the remote credential. | `one_ontology_across_every_store_and_another_repository` |
| A store failure stops only the fields of that store. The client gets no store details. The store recovers without a restart. | `a_failing_store_fails_only_its_own_fields_and_recovers` |
| `serve` publishes nothing when a store is not available. | `serve_publishes_nothing_while_a_store_is_unreachable` |
| The Apollo composition gives the same answers and the same client schema. | `apollo_composition_agrees_with_this_one` |

CI (`.github/workflows/ontology.yml`) runs on each pull request that
changes the ontology:

- The gate: format, lint with and without the database features, and
  tests.
- A build with the declared `rust-version`, 1.94.1.
- All suites in strict mode, behind each of the three gateways, with
  Postgres, DynamoDB Local, and Rover. SHA-256 checksums pin the gateway
  and Rover binaries.

## Why these libraries

- **Composition:** [`graphql-composition`](https://crates.io/crates/graphql-composition)
  (Grafbase, MPL-2.0). The `apollo-federation` crate uses the Elastic
  License 2.0, which is not compatible with GPL-3.0. MPL-2.0 is
  compatible.
- **Subgraph runtime:** [`async-graphql`](https://crates.io/crates/async-graphql)
  dynamic schemas (MIT or Apache-2.0).
- **Gateway:** not included. The supergraph uses the standard `join__`
  format. Strict gateways look for two items that graphql-composition does
  not write. `compose` adds a `@join__type` on the root query type for
  each subgraph with root fields. It also adds all definitions of the
  specs that the supergraph uses.
- **Test tools, not dependencies:** Hive Router and Hive Gateway (MIT),
  and Apollo Router (Elastic License 2.0). Rover is MIT, but its
  composition plugin uses the Elastic License 2.0.

## Roadmap

1. Registry, composition, checks, and git records. *(iteration 1)*
2. Markdown, Postgres, and DynamoDB stores. Remote sub-ontologies with
   `add-remote` and `pull`. *(iteration 2)*
3. A read-only `Page` entity for the wiki pages of the vault.
4. Mutations as git commits that `check` accepts. Writes to a branch as
   change proposals. `ontology diff <rev> <rev>`, with a class for each
   change that can break clients.
5. MCP tools and lint integration.
