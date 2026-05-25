# CLAUDE.md — AI-Assisted Development Context

This project was developed using Claude Code (Anthropic's AI coding assistant) as the primary development partner. This file documents the AI-assisted workflow, architectural decisions, and development strategy.

## Development Workflow

```
1. Requirements Analysis    → Claude analyzed the assessment brief
2. Architecture Planning    → Designed hybrid Redis-first system (see PLAN.md)
3. Implementation           → Iterative development with Claude Code
4. Code Review              → AI-assisted review found 35 issues, fixed critical bugs
5. Documentation            → All READMEs generated and maintained with AI
```

## How Claude Was Used

- **Architecture Design**: Evaluated trade-offs between pure PostgreSQL, pure Redis, and hybrid approaches. Chose Redis-first with hot/cold tiering based on latency and throughput requirements.
- **Code Generation**: All Go microservices written with Claude — domain models, Redis Lua scripts, pipeline optimizations, circuit breakers, rate limiters.
- **Code Review**: Full codebase review identified critical bugs (unchecked errors, unused Lua parameters, dead code). All fixed.
- **Documentation**: README files, architecture diagrams, and this plan document maintained throughout development.
- **Refactoring**: Migrator moved from API to dbwriter, dead code removed, go.mod versions aligned — all AI-guided.

## Key Commands Used

```bash
# Claude Code CLI
claude                          # Interactive mode
claude "fix the Redis pipeline bugs"
claude "review all code and update docs"
claude "move migrator from API to dbwriter"
```

## Project Conventions

- Go 1.24, Chi router, go-redis/v9
- All services share code via `shared/` module with `replace` directive
- Redis is the primary data store; PostgreSQL is cold storage only
- Every Redis write publishes to `persist:queue` for async PostgreSQL persistence
- Lua scripts for all atomic operations (CAS, claim, recovery)
- No `.env` files in repo — only `.env.example`
- Structured JSON logging, Prometheus metrics on all services
- Docker multi-stage builds, GitHub Actions CI per service
