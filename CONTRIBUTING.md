# Contributing to @gbyte/langgraph-checkpoint-scylladb

Thank you for your interest in contributing! This guide will help you get started.

## Development Setup

### Prerequisites

- **Node.js** ≥ 18
- **pnpm** (package manager)
- **Docker** & **Docker Compose** (for running ScyllaDB locally)

### Getting Started

```bash
# Clone the repository
git clone https://github.com/GByteTech/langgraph-checkpoint-scylladb-js.git
cd langgraph-checkpoint-scylladb-js

# Install dependencies
pnpm install

# Start ScyllaDB
docker compose up -d --wait

# Run tests
pnpm test

# Build
pnpm build
```

## Making Changes

1. **Fork** the repository and create a feature branch from `main`
2. Make your changes in `src/`
3. Add or update tests in `src/tests/`
4. Ensure all tests pass: `pnpm test`
5. Ensure the build succeeds: `pnpm build`
6. Submit a **pull request** to `main`

## Test Suite

We maintain two test suites:

- **Integration tests** (`src/tests/checkpoint.test.ts`) — Custom tests for ScyllaDB-specific behavior
- **Spec validation tests** (`src/tests/spec.test.ts`) — Official LangGraph checkpoint conformance suite

All 727 tests must pass before merging.

## Commit Convention

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add TTL refresh on read
fix: handle null checkpoint_ns in list
docs: update API reference
test: add namespace isolation tests
chore: bump scylladb-driver-alpha to 0.3
```

## Code Style

- TypeScript strict mode
- ESM modules
- Use `prettier` for formatting: `pnpm format`

## Reporting Issues

Please use [GitHub Issues](https://github.com/GByteTech/langgraph-checkpoint-scylladb-js/issues) with:

- ScyllaDB version
- Node.js version
- Steps to reproduce
- Expected vs actual behavior

---

Maintained by [GByte.Tech](https://gbyte.tech)
