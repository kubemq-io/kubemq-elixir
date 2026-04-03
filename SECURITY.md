# Security Policy

## Supported Versions

| Version | Supported |
|---------|:---------:|
| 0.1.x   | Yes       |

## Reporting a Vulnerability

If you discover a security vulnerability in the KubeMQ Elixir SDK, please report it responsibly.

### How to Report

1. **Do NOT** open a public GitHub issue for security vulnerabilities
2. Email security reports to **security@kubemq.io**
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact assessment
   - Suggested fix (if any)

### What to Expect

- **Acknowledgement**: Within 48 hours of your report
- **Assessment**: We will evaluate the severity and scope within 5 business days
- **Resolution**: Critical vulnerabilities will be patched within 7 days; others within 30 days
- **Disclosure**: We will coordinate public disclosure with you after a fix is available

### Scope

The following are in scope for security reports:

- Authentication bypass or token leakage
- TLS/mTLS configuration vulnerabilities
- Credential exposure in logs or error messages
- Denial of service via crafted messages
- Dependency vulnerabilities (gRPC, protobuf)

### Out of Scope

- KubeMQ broker vulnerabilities (report to KubeMQ server team)
- Denial of service requiring broker access
- Issues requiring physical access to the host

## Security Best Practices

When using the KubeMQ Elixir SDK:

- Always use TLS in production (`tls: [cacertfile: "..."]`)
- Use the `KubeMQ.CredentialProvider` behaviour for dynamic token management
- Do not log `auth_token` values
- Rotate authentication tokens regularly
- Use network policies to restrict access to the KubeMQ broker
