# Security

This is a personal utility for reading a home router's diagnostic dump. It is
not a hardened production service, but it is designed to keep secrets and
personal data out of the repository.

## Secrets and how they are handled

- **Router password** — read from the `VELOP_PASSWORD` environment variable at
  runtime only. It is never committed, hard-coded, logged, or written to disk.
  `run-watcher.sh` takes it as `$1` or `VELOP_PASSWORD`; the systemd service
  supplies it via a root-only `EnvironmentFile` so it never appears in `ps`.
- **`.env`** — gitignored. Keep your local config and secrets there; only
  `.env.example` (placeholders) is committed.
- **CrateDB credentials** — the Kafka Connect sink configs in `connect/*.json`
  ship `CHANGEME_CRATE_USER`/`CHANGEME_CRATE_PASSWORD` placeholders. Set them to
  your own values before registering the connectors, or externalize them with a
  Connect `ConfigProvider` (e.g. `FileConfigProvider`). Do not commit real
  credentials.

## Data privacy

- **MAC → vendor lookups are fully offline.** They resolve against a locally
  downloaded Wireshark `manuf` file; MAC addresses never leave your network.
- **`sampleoutput.txt`** is a real `sysinfo.cgi` dump used as the parser test
  fixture, but it has been **sanitized**: the public WAN IP is replaced with
  RFC 5737 documentation ranges (`203.0.113.x`, `198.51.100.x`), and all real
  SSIDs — the author's own and neighbours' networks picked up by the WiFi scan —
  are replaced with `MyWiFi` / `Neighbor-N` placeholders. The router hostname is
  genericized to `velop`. If you contribute your own captured dump, **sanitize
  it the same way first** (WAN/public IPs, SSIDs, and any device hostnames you
  consider personal).
- **TLS** — the Velop uses a self-signed certificate, so `VELOP_VERIFY_TLS`
  defaults to `false`. This is appropriate for a directly-addressed home router
  but means the connection is not certificate-verified.

## Reporting

This is a hobby project with no formal support. If you spot a sensitive value
that slipped into the repo or its history, please open an issue (without
including the sensitive value itself) so it can be scrubbed.
