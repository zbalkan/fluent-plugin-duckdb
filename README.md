# fluent-plugin-duckdb

Fluentd output plugin for [DuckDB](https://duckdb.org).
Stores Fluentd events into a DuckDB database using a JSON column, allowing you to run rich SQL queries directly on logs.

---

## Requirements

| fluent-plugin-duckdb | Fluentd | Ruby   | DuckDB           |
| -------------------- | ------- | ------ | ---------------- |
| >= 0.1.0             | >= v1.0 | >= 2.5 | >= 1.4.1 (C API) |

---

## Pre-requisites (Linux)

The [Ruby `duckdb`](https://rubygems.org/gems/duckdb) gem depends on the **DuckDB C API**. Install the shared library and headers first:

```bash
wget https://github.com/duckdb/duckdb/releases/download/v1.4.1/libduckdb-linux-amd64.zip
unzip libduckdb-linux-amd64.zip -d libduckdb
sudo mv libduckdb/duckdb.* /usr/local/include/
sudo mv libduckdb/libduckdb.so /usr/local/lib/
sudo ldconfig /usr/local/lib
```

---

## Installation

```bash
sudo fluent-gem install duckdb -v 1.4.1.0
sudo fluent-gem install yajl-ruby
```

Then copy the plugin into Fluentd's plugin directory:

```bash
sudo cp lib/fluent/plugin/out_duckdb.rb /etc/fluent/plugin/
```

---

## Configuration

> This output is **buffered**. Buffering, flushing cadence, and retries are controlled by Fluentd’s `<buffer>` section. The plugin inserts each chunk in a single DuckDB transaction. You can use a buffer or storage plugin for high volume buffer needs.

```xml
<match **>
  @type duckdb
  database /var/log/ducklogs.duckdb
  table fluentd_events
  time_col time
  tag_col tag
  record_col record
  # Optional: dedupe true enables idempotent inserts via a unique hash
  # dedupe true

  # Buffering is handled by Fluentd:
  <buffer tag,time>
    @type memory
    timekey 30s
    timekey_wait 5s
    # add further buffer settings (flush_interval, retry, file buffer, etc.) as needed
  </buffer>
</match>
```

All columns are customizable. The plugin creates the table if it does not exist.

---

## Schema

The plugin defines the table schema as follows:

| Column   | Type      |
| -------- | --------- |
| `tag`    | VARCHAR   |
| `time`   | TIMESTAMP |
| `record` | JSON      |

If `dedupe true` is set, the plugin also adds an internal `record_hash` column and a `UNIQUE(tag, time, record_hash)` constraint to make retries idempotent.

---

## Notes on Encoding

The plugin uses `Yajl` for JSON encoding.
Encoding is automatic and unconditional. If `yajl-ruby` is not installed, it will raise an error.

---

## Querying Example

```sql
SELECT record->>'$.user.id' AS user_id
FROM fluentd_events
WHERE record->>'$.event' = 'login';

SELECT record->>'$.host' AS host, COUNT(*) AS c
FROM fluentd_events
GROUP BY host;
```

---

## Smoke Test

### 1. Minimal Config with Dummy Input

```xml
<source>
  @type dummy
  tag test.duckdb
  auto_increment_key id
  rate 1
  dummy [
    {"event":"login","user":{"id":123,"name":"alice"}},
    {"event":"logout","user":{"id":456,"name":"bob"}}
  ]
</source>

<match test.duckdb>
  @type duckdb
  database /tmp/fluentd.duckdb
  table fluentd_events

  <buffer tag,time>
    @type memory
    timekey 30s
    timekey_wait 5s
  </buffer>
</match>
```

### 2. Run Fluentd

```bash
fluentd -c fluent.conf -vv
```

### 3. Inspect Results

```bash
duckdb /tmp/fluentd.duckdb

SELECT record->>'$.user.name', record->>'$.event' FROM fluentd_events;
```

---

## Testing

Use the included test with `test-unit` and `fluent/test/driver/output`.

```bash
bundle install
rake test
```

Example test verifies correct insertion and retrieval using JSON path queries:

```ruby
SELECT record->>'$.user.id' FROM test_events;
```

---

## License

Apache License 2.0
