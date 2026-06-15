# 51: HTTP Adapter

The HTTP adapter sends requests via `reqwest`. Included in the
core `nbrs` binary.

---

## Configuration

```yaml
params:
  base_url: "http://localhost:8080"
  timeout: "30000"    # ms
```

Or per-op:
```yaml
ops:
  get_item:
    adapter: http
    method: GET
    uri: "{base_url}/items/{item_id}"
```

---

## HttpConfig

```rust
pub struct HttpConfig {
    pub base_url: Option<String>,
    pub timeout_ms: u64,
    pub follow_redirects: bool,
}
```

One `reqwest::Client` per activity, shared via `Arc`. The client
manages its own connection pool.

---

## Op Fields

| Field | Description |
|-------|-------------|
| `method` | HTTP method (GET, POST, PUT, DELETE) |
| `uri` | Request URL (bind points resolved) |
| `body` | Request body (bind points resolved) |
| `headers` | Additional headers |

All fields support `{name}` bind point substitution.

---

## Error Names

| Error Name | Scope | Description |
|-----------|-------|-------------|
| `Timeout` | Op | Request timed out |
| `ConnectionRefused` | Adapter | Target unreachable |
| `RequestError` | Op | Request build/send failure |
| `HttpStatus{code}` | Op | Non-2xx response |
