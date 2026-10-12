# Command line interface

The `redisjobs` console script (installed with the package) exposes three
subcommands. The default Redis URL is `redis://localhost:6379/0`; override it
with `--url`.

## `redisjobs worker`

Run a worker consuming one or more queues.

```bash
redisjobs worker -q default -q emails -c 8 --app myapp.tasks
```

| Option | Default | Description |
| --- | --- | --- |
| `-q, --queue` | config default | Queue to consume; repeatable. |
| `-c, --concurrency` | `10` | Maximum concurrent jobs. |
| `--url` | `redis://localhost:6379/0` | Redis URL. |
| `-a, --app` | — | Python module(s) to import so tasks register; repeatable. |
| `--log-level` | `INFO` | Logging level. |
| `--no-json-logs` | off | Emit human-readable logs instead of JSON. |

The worker handles `SIGINT`/`SIGTERM` for graceful shutdown.

## `redisjobs enqueue`

Enqueue a task by name (useful for smoke-testing and ops).

```bash
redisjobs enqueue myapp.tasks:send_welcome 42 --queue default --priority 1
redisjobs enqueue myapp.tasks:render '{"a": 1}' --delay 30
```

Arguments are parsed as JSON when possible, otherwise passed as strings. Prints
a JSON object with the created job's `id`, `task`, `queue` and `status`.

| Option | Default | Description |
| --- | --- | --- |
| `-q, --queue` | config default | Target queue. |
| `-p, --priority` | `5` | Integer priority (lower = sooner). |
| `-d, --delay` | — | Delay in seconds. |
| `--url` | `redis://localhost:6379/0` | Redis URL. |

## `redisjobs stats`

Print queue statistics as JSON.

```bash
redisjobs stats --queue default
```

```json
{
  "queue": "default",
  "ready": { "0": 2, "9": 1 },
  "total_ready": 3,
  "delayed": 1,
  "active": 4,
  "dead": 0
}
```
