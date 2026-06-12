# zerodb-prefect

**ZeroDB event sensor and result storage for [Prefect](https://www.prefect.io/).**

[![PyPI](https://img.shields.io/pypi/v/zerodb-prefect)](https://pypi.org/project/zerodb-prefect/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Why use this?

| Feature | Description |
|---------|-------------|
| **Event sensor** | ZeroDB events trigger Prefect flows automatically |
| **Result storage** | Store and retrieve flow results in ZeroDB tables |
| **Auto-provision** | No signup needed -- ZeroDB project created on first use |
| **Webhook + polling** | Two modes: poll event stream or receive webhooks |
| **Credentials block** | Manage API keys with the ZeroDBCredentials class |

## Installation

```bash
pip install zerodb-prefect
```

## Quick Start

### Event Sensor

```python
from zerodb_prefect import ZeroDBSensor

sensor = ZeroDBSensor(event_type='zerodb.vector.stored')

@sensor.on_event
def process_vector(event):
    print(f"New vector: {event.data}")
    return {'processed': event.data['vector_id']}

# Start polling for events
sensor.start()
```

### Result Storage

```python
from zerodb_prefect import ZeroDBBlock

block = ZeroDBBlock()

# Write results
block.write_path('/results/my-flow/run-1', {
    'status': 'complete',
    'rows_processed': 42,
})

# Read results
data = block.read_path('/results/my-flow/run-1')
print(data)  # {'status': 'complete', 'rows_processed': 42}
```

### Credentials

```python
from zerodb_prefect import ZeroDBCredentials

creds = ZeroDBCredentials(api_key='zdb_...', project_id='proj_...')
client = creds.get_client()  # Authenticated requests.Session
```

## API Reference

### `ZeroDBSensor(event_type, **kwargs)`

Poll ZeroDB events and trigger handlers.

| Param | Default | Description |
|-------|---------|-------------|
| `event_type` | required | Event type to listen for |
| `api_key` | auto | ZeroDB API key |
| `project_id` | auto | ZeroDB project ID |
| `poll_interval` | 5 | Seconds between polls |
| `batch_size` | 100 | Max events per poll |

**Methods:**

| Method | Description |
|--------|-------------|
| `@sensor.on_event` | Decorator to register handler |
| `sensor.add_handler(func)` | Register handler (non-decorator) |
| `sensor.poll()` | Manually poll for events |
| `sensor.start()` | Start polling in background thread |
| `sensor.stop()` | Stop polling |
| `sensor.process_webhook(payload)` | Process webhook payload |

### `ZeroDBBlock(**kwargs)`

Store and retrieve Prefect flow results.

| Method | Description |
|--------|-------------|
| `write_path(path, content)` | Store content keyed by path |
| `read_path(path)` | Read content by path |
| `exists(path)` | Check if path exists |
| `delete_path(path)` | Delete stored result |
| `list_paths(prefix=None)` | List stored paths |

### `ZeroDBCredentials(**kwargs)`

Manage ZeroDB API credentials.

| Method | Description |
|--------|-------------|
| `get_client()` | Return authenticated requests.Session |
| `get_headers()` | Return auth headers as dict |

## Configuration

### Environment Variables

```bash
export ZERODB_API_KEY="your-api-key"
export ZERODB_PROJECT_ID="your-project-id"
# Optional
export ZERODB_BASE_URL="https://api.ainative.studio"
```

### Auto-Provisioning

If no credentials are found, `zerodb-prefect` automatically creates a free ZeroDB project on first use. Credentials are saved to `~/.zerodb/config.json`.

## Use Cases

### ML Pipeline Results

```python
from zerodb_prefect import ZeroDBBlock

block = ZeroDBBlock()

# After training
block.write_path('/models/v2/metrics', {
    'accuracy': 0.94,
    'f1': 0.91,
    'training_time': 3600,
})

# In evaluation flow
metrics = block.read_path('/models/v2/metrics')
```

### Event-Driven ETL

```python
from zerodb_prefect import ZeroDBSensor, ZeroDBBlock

sensor = ZeroDBSensor(event_type='zerodb.file.uploaded')
block = ZeroDBBlock()

@sensor.on_event
def etl_pipeline(event):
    file_path = event.data.get('path')
    block.write_path(f'/etl/processed/{file_path}', {
        'source': file_path,
        'status': 'processed',
    })

sensor.start()
```

---

**Powered by [ZeroDB](https://zerodb.dev) + [AINative](https://ainative.studio)**

Free database for AI agents. Auto-provisions in 200ms. No signup required.

[Get started](https://ainative.studio) | [Documentation](https://docs.ainative.studio) | [ZeroDB](https://zerodb.dev) | [GitHub](https://github.com/AINative-Studio/zerodb-prefect)

## License

MIT

---

## Zero-Config Auto-Provisioning

**No account needed to start.** On first run, this package automatically:

1. Provisions a **free ZeroDB instance** (~800ms)
2. Saves credentials locally (`.env`, `.mcp.json`)
3. Prints a **claim link** in your terminal

### The 72-Hour Window

Your auto-provisioned instance is **free for 72 hours**. To keep your data permanently, visit the **claim URL** printed in your terminal.

### For AI Agents

If a coding agent (Claude Code, Cursor, Windsurf) installs this package:
- Auto-provisions without human intervention
- The **claim URL** appears in agent output — visit it to take ownership
- `.claude/CLAUDE.md` instructs agents to surface the claim link to you
- Check `.env` or `~/.ainative/config.json` for your project ID if you miss it
