[![CI](https://github.com/infrasonar/wmi-probe/workflows/CI/badge.svg)](https://github.com/infrasonar/wmi-probe/actions)
[![Release Version](https://img.shields.io/github/release/infrasonar/wmi-probe)](https://github.com/infrasonar/wmi-probe/releases)

# InfraSonar WMI Probe

## Environment variable

Variable          | Default                        | Description
----------------- | ------------------------------ | ------------
`AGENTCORE_HOST`  | `127.0.0.1`                    | Hostname or Ip address of the AgentCore.
`AGENTCORE_PORT`  | `8750`                         | AgentCore port to connect to.
`INFRASONAR_CONF` | `/data/config/infrasonar.yaml` | File with probe and asset configuration like credentials.
`LOG_LEVEL`       | `warning`                      | Log level (`debug`, `info`, `warning`, `error` or `critical`).
`LOG_COLORIZED`   | `0`                            | Log using colors (`0`=disabled, `1`=enabled).
`LOG_FTM`         | `%y%m%d %H:%M:%S`              | Log format prefix.

## Docker build

```
docker build -t wmi-probe . --no-cache
```
