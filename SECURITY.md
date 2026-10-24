# Security Policy

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, report them privately:

- Use **[GitHub Security Advisories](https://github.com/ubgo/workflow/security/advisories/new)** (preferred), or
- Email **khanakia@gmail.com** with the details.

Please include:

- A description of the vulnerability and its impact.
- Steps to reproduce (proof-of-concept if possible).
- The commit or version, the driver (memory, sqlstore and its database, Hatchet) and the environment.

We will acknowledge your report within a few days and keep you updated on remediation. We ask that you give us a reasonable window to release a fix before any public disclosure, and we're happy to credit you in the advisory.

## Supported versions

Nothing is tagged yet. Until the first release, security fixes land on `main` only.

| Version | Supported |
|---|---|
| `main` | ✅ |

Once releases exist, fixes are applied to the latest `v0.x`; under SemVer a `v0.x` release may break, so fixes are not backported to an earlier `v0.x`.

## Scope

In scope: a job payload, step output, signal payload or schedule input reaching SQL text instead of a bound parameter in `sqlstore` or `sqlstore/outbox`; a run, lease or lock claimed by two workers when the driver declares it is claimed once; a payload sealed by `middleware.WithCodec` that leaves the process unsealed.

Out of scope: what a step handler does with its payload, which is application code, and the security of the backend itself (the database server, the Hatchet engine) and of its credentials.
