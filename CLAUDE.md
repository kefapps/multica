# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Super Multica is a distributed AI agent framework with a monorepo architecture. It includes an agent engine with multi-provider LLM support, a WebSocket gateway, a console hub for multi-agent coordination, and frontend apps (Next.js web, Electron desktop).

## Monorepo Structure

- **`src/`** — Core modules (agent engine, gateway, console, client, shared types)
- **`apps/web`** — Next.js 16 web app (`@multica/web`, port 3001)
- **`apps/desktop`** — Electron + Vite + React desktop app (`@multica/desktop`)
- **`packages/ui`** — Shared UI component library (`@multica/ui`, Shadcn/Tailwind CSS v4)
- **`packages/sdk`** — Gateway client SDK (`@multica/sdk`, Socket.io)
- **`packages/store`** — Zustand state management (`@multica/store`)
- **`skills/`** — Bundled agent skills (commit, code-review, skill-creator)

## Common Commands

```bash
# Install dependencies
pnpm install

# Development (all services concurrently: gateway:3000, console, web:3001)
pnpm dev

# Individual services
pnpm dev:gateway          # WebSocket gateway only
pnpm dev:console          # NestJS console with agent
pnpm dev:web              # Next.js web app
pnpm dev:desktop          # Electron desktop app

# Agent CLI
pnpm agent:cli            # Non-interactive agent
pnpm agent:interactive    # Interactive REPL mode

# Build (turbo-orchestrated)
pnpm build

# Type checking
pnpm typecheck

# Testing (vitest, tests live in src/**/*.test.ts)
pnpm test                 # Single run
pnpm test:watch           # Watch mode
pnpm test:coverage        # With v8 coverage
```

## Architecture

```
Frontend (web:3001 / desktop)
  → @multica/sdk (GatewayClient, Socket.io)
    → Gateway (NestJS, WebSocket, port 3000)
      → Console Hub (multi-agent coordination)
        → Agent Engine (LLM runner, sessions, skills, tools)
```

**Agent Engine** (`src/agent/`): Orchestrates LLM interactions with multi-provider support (OpenAI, Anthropic, DeepSeek, Kimi, Groq, Mistral, Google, Together). Features session management (JSONL-based, UUIDv7 IDs), profile system (`~/.super-multica/agent-profiles/`), modular skills with hot-reload, and token-aware context window guards (compaction modes: tokens, count, summary).

**Gateway** (`src/gateway/`): NestJS WebSocket server with Socket.io for real-time message passing, RPC request/response, and streaming.

**Console** (`src/console/`): NestJS hub for multi-agent coordination with a web dashboard.

## Tech Stack & Config

- **Package manager**: pnpm 10 with workspaces (`pnpm-workspace.yaml`)
- **Build orchestration**: Turborepo (`turbo.json`)
- **TypeScript**: ESNext target, NodeNext modules, strict mode, `verbatimModuleSyntax`, `experimentalDecorators` (NestJS)
- **Testing**: Vitest with globals enabled, node environment
- **Frontend**: React 19, Next.js 16, Tailwind CSS v4, Shadcn/UI (zinc base, hugeicons)
- **Backend**: NestJS 11, Socket.io, Pino logging
- **CLI bundling**: esbuild → `bin/` directory

## Environment Setup

```bash
cp .env.example .env
# Set LLM_PROVIDER and corresponding API keys
# Supported: openai, anthropic, deepseek, kimi-coding, groq, mistral, together, google
# .env is loaded automatically via --env-file in all dev/agent scripts
```

## Atomic Commits

After completing any task that modifies code, create atomic commits without asking for permission.

1. Run `git status` and `git diff` to see all modifications
2. Skip if no changes exist
3. Group changes by logical purpose (feature, fix, refactor, docs, test, chore)
4. Stage and commit each group separately

**Format**: Conventional commits — `<type>(<scope>): <description>`

Types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`

**Rules**:
- Each commit should be independently meaningful and buildable
- Related test files go with their implementation
- Never create empty commits or combine unrelated changes
- `git commit --amend` only for immediate small fixes to the last commit
