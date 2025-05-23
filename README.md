[![CI](https://github.com/infrasonar/wmi-probe/workflows/CI/badge.svg)](https://github.com/infrasonar/wmi-probe/actions)
[![Release Version](https://img.shields.io/github/release/infrasonar/wmi-probe)](https://github.com/infrasonar/wmi-probe/releases)

# InfraSonar WMI Probe

## Environment variable

Variable            | Default                        | Description
------------------- | ------------------------------ | ------------
`AGENTCORE_HOST`    | `127.0.0.1`                    | Hostname or Ip address of the AgentCore.
`AGENTCORE_PORT`    | `8750`                         | AgentCore port to connect to.
`INFRASONAR_CONF`   | `/data/config/infrasonar.yaml` | File with probe and asset configuration like credentials.
`MAX_PACKAGE_SIZE`  | `500`                          | Maximum package size in kilobytes _(1..2000)_.
`MAX_CHECK_TIMEOUT` | `300`                          | Check time-out is 80% of the interval time with `MAX_CHECK_TIMEOUT` in seconds as absolute maximum.
`DRY_RUN`           | _none_                         | Do not run demonized, just return checks and assets specified in the given yaml _(see the [Dry run section](#dry-run) below)_.
`LOG_LEVEL`         | `warning`                      | Log level (`debug`, `info`, `warning`, `error` or `critical`).
`LOG_COLORIZED`     | `0`                            | Log using colors (`0`=disabled, `1`=enabled).
`LOG_FMT`           | `%y%m%d %H:%M:%S`              | Log format prefix.

## Docker build

```
docker build -t wmi-probe . --no-cache
```

## Config

```yaml
wmi:
  config:
    username: "my_account@domain"
    password: "my_password"
```

## Dry run

Available checks:
- `memory`
- `network`
- `netstat`
- `ntDomain`
- `process`
- `services`
- `software`
- `storage`
- `system`
- `updates`
- `users`

Create a yaml file, for example _(test.yaml)_:

```yaml
asset:
  name: "foo.local"
  check: "system"
  config:
    address: "192.168.1.2"
```

Run the probe with the `DRY_RUN` environment variable set the the yaml file above.

```
DRY_RUN=test.yaml python main.py
```
