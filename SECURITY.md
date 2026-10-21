# Security Policy

## Supported Versions

Quarry is pre-1.0. Security fixes ship in a new patch release of the latest
minor version; older versions do not receive backports. Upgrade to the
latest release to pick up fixes.

| Version | Supported |
|---------|-----------|
| Latest release | Yes |
| Older releases | No |

## Reporting a Vulnerability

Report vulnerabilities privately. Do **not** open a public issue, pull
request, or discussion for a suspected vulnerability.

- **GitHub (preferred)**: [report a vulnerability](https://github.com/pithecene/quarry/security/advisories/new) via private vulnerability reporting
- **Email**: contact@pithecene.sh

Include:

1. Affected version (`quarry version`) and component (CLI/runtime, executor, SDK, container image)
2. Description of the issue and its impact
3. Reproduction steps or a proof of concept
4. Any known mitigation

## Disclosure

Confirmed vulnerabilities are fixed in a patch release and disclosed through
a GitHub Security Advisory and the release's `CHANGELOG.md` entry under
**Security**. Advisories in dependencies that have no fix yet are tracked in
[`SUPPORT.md`](SUPPORT.md) § Open Security Advisories.
