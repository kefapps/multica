# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

Multica is an AI-native task management platform — like Linear, but with AI agents as first-class citizens.

- Agents can be assigned issues, create issues, comment, and change status
- Supports local (daemon) and cloud agent runtimes
- Built for 2-10 person AI-native teams

## Architecture

**Go backend + monorepo frontend (pnpm workspaces + Turborepo) with shared packages.**

- `server/` — Go backend (Chi router, sqlc for DB, gorilla/websocket for real-time)
- `apps/web/` — Next.js 16 frontend (App Router)
- `apps/desktop/` — Electron 39 desktop app (electron-vite + react-router-dom)
- `packages/core/` — Headless business logic (zero react-dom, all-platform reuse)
- `packages/ui/` — Atomic UI components (zero business logic)
- `packages/views/` — Shared business pages/components (zero next/* imports, zero react-router imports)
- `packages/tsconfig/` — Shared TypeScript configuration

### Monorepo Tooling

- **pnpm workspaces** for dependency management. `pnpm-workspace.yaml` defines a `catalog:` for version pinning — all shared deps (React, Zustand, TanStack Query, Tailwind, TypeScript) use `catalog:` references to guarantee a single version across all packages.
- **Turborepo** for task orchestration — build, typecheck, test, lint all respect the package dependency graph.
- **Internal Packages pattern** — all shared packages export raw `.ts`/`.tsx` files (no pre-compilation). The consuming app's bundler (Vite for desktop, Next.js for web) compiles them directly. This gives zero-config HMR and instant go-to-definition. If a package is ever published to npm, add a build step then.

### Package Architecture

Three shared packages with single-direction dependencies:

```
packages/
├── core/     # @multica/core  — types, API client, stores, queries, mutations, realtime, platform
├── ui/       # @multica/ui    — 55 shadcn components, common components, markdown, hooks, styles
├── views/    # @multica/views — issue pages, editor, modals, skills, runtimes, navigation, layout, auth, settings
└── tsconfig/ # @multica/tsconfig — shared TS base configs
```

**Dependency direction:** `views/ → core/ + ui/`. Core and UI are independent of each other. No package imports from `next/*`, `react-router-dom`, or app-specific code.

**Platform bridge:** `packages/core/platform/` provides `CoreProvider` — a single component that initializes API client, auth/workspace stores, WS connection, and QueryClient. Each app wraps its root with `<CoreProvider apiBaseUrl wsUrl>` and provides its own `NavigationAdapter` for routing.

```
apps/web:     ThemeProvider > CoreProvider(onLogin=cookie, onLogout=cookie) > WebNavigationProvider > pages
apps/desktop: ThemeProvider > CoreProvider(apiBaseUrl, wsUrl)               > RouterProvider > DesktopNavigationProvider > pages
```

### packages/core/ (`@multica/core`)

Headless business logic. **Zero react-dom, zero localStorage, zero process.env.**

| Module | Purpose | Key exports |
|---|---|---|
| `core/types/` | Domain types + StorageAdapter interface | `Issue`, `Agent`, `Workspace`, `StorageAdapter` |
| `core/api/` | API client class + WS client | `ApiClient`, `WSClient`, `setApiInstance()` |
| `core/auth/` | Auth store factory | `createAuthStore(options)`, `registerAuthStore()` |
| `core/workspace/` | Workspace store factory + actor hooks | `createWorkspaceStore(api)`, `useActorName()` |
| `core/issues/` | Issue queries, mutations, stores, config | `issueListOptions`, `useUpdateIssue`, `useIssueStore` |
| `core/inbox/` | Inbox queries, mutations, WS updaters | `inboxListOptions`, `useMarkInboxRead` |
| `core/runtimes/` | Runtime queries + mutations | `runtimeListOptions`, `useDeleteRuntime` |
| `core/realtime/` | WS provider + sync hooks | `WSProvider`, `useWSEvent`, `useRealtimeSync` |
| `core/hooks.tsx` | Workspace ID context | `useWorkspaceId`, `WorkspaceIdProvider` |
| `core/modals/` | Modal state store | `useModalStore` |
| `core/navigation/` | Navigation state store | `useNavigationStore` |
| `core/platform/` | CoreProvider + auth init + default storage | `CoreProvider`, `AuthInitializer`, `defaultStorage` |

**Store factory pattern:** Auth and workspace stores are created via factory functions that receive platform-specific dependencies:
```typescript
createAuthStore({ api, storage, onLogin?, onLogout? })
createWorkspaceStore(api, { storage?, onError? })
```
Each app creates its own instances in its platform layer and registers them via `registerAuthStore()` / `registerWorkspaceStore()`.

**StorageAdapter:** All persistent storage goes through a `StorageAdapter` interface (getItem/setItem/removeItem), injected by the platform. Web uses an SSR-safe localStorage wrapper.

### packages/ui/ (`@multica/ui`)

Atomic UI layer. **Zero business logic, zero `@multica/core` imports.**

- `components/ui/` — 55 shadcn components (button, dialog, card, tooltip, sidebar, etc.)
- `components/common/` — Pure-props components (actor-avatar, emoji-picker, reaction-bar, multica-icon, theme-provider)
- `markdown/` — Markdown renderer with `renderMention` slot for platform-specific mention cards
- `hooks/` — DOM hooks (use-auto-scroll, use-mobile, use-scroll-fade)
- `lib/utils.ts` — `cn()` function (clsx + tailwind-merge)
- `styles/tokens.css` — Tailwind CSS v4 design tokens (@theme inline, :root, .dark variables)
- `styles/base.css` — Shared base layer (scrollbar, shiki themes, entrance-spin animation, sidebar active state, sonner alignment, body/html defaults)

### packages/views/ (`@multica/views`)

Shared business UI pages. **Zero `next/*` imports. Zero `react-router-dom` imports.** Uses `NavigationAdapter` for routing.

- `navigation/` — `NavigationAdapter` interface, `useNavigation()` hook, `AppLink` component
- `layout/` — `DashboardLayout`, `AppSidebar`, `useDashboardGuard`
- `auth/` — `LoginPage` (shared login with optional Google OAuth via props)
- `issues/components/` — IssuesPage, IssueDetail, BoardView, ListView, pickers, icons
- `editor/` — ContentEditor, TitleEditor, Tiptap extensions
- `modals/` — CreateIssueModal, CreateWorkspaceModal, ModalRegistry
- `my-issues/`, `skills/`, `runtimes/`, `agents/`, `inbox/`, `settings/` — domain pages
- `common/` — Data-aware wrappers (ActorAvatar with useActorName, Markdown with IssueMentionCard)

**NavigationAdapter:** Platform-agnostic routing interface. All shared components use `useNavigation()` and `<AppLink>` — never import from `next/navigation` or `react-router-dom` directly. Optional methods (`openInNewTab`, `getShareableUrl`) are provided by desktop only; shared code checks their existence before calling.

### apps/web/ (Next.js App)

Thin routing shells + platform-specific code.

```
apps/web/
├── app/              # Next.js route shells (< 15 lines each, import from @multica/views)
├── platform/         # Web platform bridge — only navigation.tsx remains
├── features/
│   ├── auth/         # Web-only: auth-cookie.ts (cookie for Next.js middleware)
│   ├── landing/      # Web-only: landing pages (uses next/image, next/link)
│   └── search/       # Web-only: search dialog
└── components/       # App-level: web-providers.tsx, locale-sync, loading-indicator
```

**`platform/navigation.tsx`** — `WebNavigationProvider` wrapping Next.js `useRouter`/`usePathname`. The only web-platform-specific file remaining — core initialization is handled by `CoreProvider` in `packages/core/platform/`.

### apps/desktop/ (Electron App)

Electron 39 + electron-vite + react-router-dom. Uses `createHashRouter` since there's no server for pushState.

Desktop shares all page components from `@multica/views` — the router imports `IssuesPage`, `InboxPage`, `AgentsPage`, etc. directly. Desktop-specific code is limited to: layout shell (tab bar, traffic light region), navigation adapter, and page wrappers for dynamic `document.title`.

**Key conventions:**
- New routes must include `handle: { title: "..." }` for automatic tab titles
- Pages with dynamic titles (e.g. issue detail) use `useDocumentTitle(title)` to override
- `platform/navigation.tsx` adapts react-router to `NavigationAdapter` — the only place that imports from `react-router-dom`
- Environment variables (`VITE_API_URL`, `VITE_WS_URL`) are baked in at build time via `.env.production`

### State Management

- **TanStack Query** for all server state — issues, inbox, members, agents, skills, runtimes. Query definitions in `@multica/core/<domain>/queries.ts`, mutations in `mutations.ts`.
- **Zustand** for client-only state — UI selections (`activeIssueId`), view filters, modal state. Auth and workspace stores use factory pattern with injected dependencies.
- **React Context** for `WorkspaceIdProvider` (provides workspace ID to all dashboard children) and `NavigationProvider` (provides platform-agnostic routing).
- **Local `useState`** for component-scoped UI state (forms, modals, filters).

**TanStack Query conventions:**
- `staleTime: Infinity` — WS events handle cache freshness, no polling or refetch-on-focus.
- WS events trigger `queryClient.invalidateQueries()` (preferred) or `queryClient.setQueryData()` for granular updates.
- All workspace-scoped query keys include `wsId` — workspace switch automatically uses new cache.
- Mutations use `onMutate` for optimistic updates + `onError` for rollback + `onSettled` for invalidation.

**Zustand store conventions:**
- Stores in `@multica/core` hold only client state. Zero direct `api.*` calls — API access is injected via factory.
- Auth/workspace stores are created by platform layer and registered via `registerAuthStore()` / `registerWorkspaceStore()`.
- Other stores (issue, modal, navigation) are plain Zustand stores exported directly.

### Import Conventions

```typescript
// Core (headless business logic) — from @multica/core
import { issueListOptions } from "@multica/core/issues/queries";
import { useUpdateIssue } from "@multica/core/issues/mutations";
import { useWorkspaceId } from "@multica/core/hooks";
import type { Issue } from "@multica/core/types";

// UI (atomic components) — from @multica/ui
import { Button } from "@multica/ui/components/ui/button";
import { cn } from "@multica/ui/lib/utils";
import { ActorAvatar } from "@multica/ui/components/common/actor-avatar";

// Views (shared pages) — from @multica/views
import { IssuesPage } from "@multica/views/issues/components";
import { useNavigation, AppLink } from "@multica/views/navigation";
import { ModalRegistry } from "@multica/views/modals/registry";

// Platform (web-only) — from @/platform
import { WebNavigationProvider } from "@/platform/navigation";

// Platform (desktop-only) — from @/ (maps to apps/desktop/src/renderer/src/)
import { useTabStore } from "@/stores/tab-store";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { DesktopNavigationProvider } from "@/platform/navigation";

// Web-only features — from @/features
import { SearchCommand } from "@/features/search";
```

`@/` maps to `apps/web/` in the web app and `apps/desktop/src/renderer/src/` in the desktop app. Within a package, use relative imports. Between packages, use `@multica/*`.

### Data Flow

```
Browser → useQuery (@multica/core) → ApiClient (@multica/core/api) → REST API → sqlc → PostgreSQL
Browser ← useQuery cache ← invalidateQueries ← WS event handlers ← WSClient ← Hub.Broadcast()
```

Mutations: `useMutation (@multica/core)` → optimistic cache update → API call → onSettled invalidation.
WS events: `use-realtime-sync.ts` → `queryClient.invalidateQueries()` for most events, `setQueryData()` for granular issue/inbox updates.

### Backend Structure (`server/`)

- **Entry points** (`cmd/`): `server` (HTTP API), `multica` (CLI — daemon, agent management, config), `migrate`
- **Handlers** (`internal/handler/`): One file per domain (issue, comment, agent, auth, daemon, etc.). Each handler holds `Queries`, `DB`, `Hub`, and `TaskService`.
- **Real-time** (`internal/realtime/`): Hub manages WebSocket clients. Server broadcasts events; inbound WS message routing is still TODO.
- **Auth** (`internal/auth/` + `internal/middleware/`): JWT (HS256). Middleware sets `X-User-ID` and `X-User-Email` headers. Login creates user on-the-fly if not found.
- **Task lifecycle** (`internal/service/task.go`): Orchestrates agent work — enqueue → claim → start → complete/fail. Syncs issue status automatically and broadcasts WS events at each transition.
- **Agent SDK** (`pkg/agent/`): Unified `Backend` interface for executing prompts via Claude Code or Codex. Each backend spawns its CLI and streams results via `Session.Messages` + `Session.Result` channels.
- **Daemon** (`internal/daemon/`): Local agent runtime — auto-detects available CLIs (claude, codex), registers runtimes, polls for tasks, routes by provider.
- **CLI** (`internal/cli/`): Shared helpers for the `multica` CLI — API client, config management, output formatting.
- **Events** (`internal/events/`): Internal event bus for decoupled communication between handlers and services.
- **Logging** (`internal/logger/`): Structured logging via slog. `LOG_LEVEL` env var controls level (debug, info, warn, error).
- **Database**: PostgreSQL with pgvector extension (`pgvector/pgvector:pg17`). sqlc generates Go code from SQL in `pkg/db/queries/` → `pkg/db/generated/`. Migrations in `migrations/`.
- **Routes** (`cmd/server/router.go`): Public routes (auth, health, ws) + protected routes (require JWT) + daemon routes (unauthenticated, separate auth model).

### Multi-tenancy

All queries filter by `workspace_id`. Membership checks gate access. `X-Workspace-ID` header routes requests to the correct workspace.

### Agent Assignees

Assignees are polymorphic — can be a member or an agent. `assignee_type` + `assignee_id` on issues. Agents render with distinct styling (purple background, robot icon).

## Commands

```bash
# One-click setup & run
make setup            # First-time: ensure shared DB, create app DB, migrate
make start            # Start backend + frontend together
make stop             # Stop app processes for the current checkout
make db-down          # Stop the shared PostgreSQL container

# Frontend (all commands go through Turborepo)
pnpm install
pnpm dev:web          # Next.js dev server (port 3000)
pnpm dev:desktop      # Electron dev (electron-vite, HMR)
pnpm build            # Build all frontend apps
pnpm typecheck        # TypeScript check (all packages + apps via turbo)
pnpm lint             # ESLint via Next.js
pnpm test             # TS tests (Vitest, via turbo)

# Backend (Go)
make dev              # Run Go server (port 8080)
make daemon           # Run local daemon
make build            # Build server + CLI binaries to server/bin/
make cli ARGS="..."   # Run multica CLI (e.g. make cli ARGS="config")
make test             # Go tests
make sqlc             # Regenerate sqlc code after editing SQL in server/pkg/db/queries/
make migrate-up       # Run database migrations
make migrate-down     # Rollback migrations

# Run a single Go test
cd server && go test ./internal/handler/ -run TestName

# Run a single TS test
pnpm --filter @multica/web exec vitest run src/path/to/file.test.ts

# Run a single E2E test (requires backend + frontend running)
pnpm exec playwright test e2e/tests/specific-test.spec.ts

# Desktop build & package
pnpm --filter @multica/desktop build      # Compile TS → JS (reads .env.production)
pnpm --filter @multica/desktop package    # Package into .app/.dmg/.exe (current platform only)

# shadcn (monorepo mode — must specify app)
npx shadcn add badge -c apps/web

# Infrastructure
make db-up            # Start shared PostgreSQL (pgvector/pg17 image)
make db-down          # Stop shared PostgreSQL
```

### CI Requirements

CI runs on Node 22 and Go 1.26.1 with a `pgvector/pgvector:pg17` PostgreSQL service. See `.github/workflows/ci.yml`.

### Worktree Support

All checkouts share one PostgreSQL container. Isolation is at the database level — each worktree gets its own DB name and unique ports via `.env.worktree`. Main checkouts use `.env`.

```bash
make worktree-env       # Generate .env.worktree with unique DB/ports
make setup-worktree     # Setup using .env.worktree
make start-worktree     # Start using .env.worktree
```

## Coding Rules

- TypeScript strict mode is enabled; keep types explicit.
- Go code follows standard Go conventions (gofmt, go vet).
- Keep comments in code **English only**.
- Prefer existing patterns/components over introducing parallel abstractions.
- Unless the user explicitly asks for backwards compatibility, do **not** add compatibility layers, fallback paths, dual-write logic, legacy adapters, or temporary shims.
- If a flow or API is being replaced and the product is not yet live, prefer removing the old path instead of preserving both old and new behavior.
- Treat compatibility code as a maintenance cost, not a default safety mechanism. Avoid "just in case" branches that make the codebase harder to reason about.
- Avoid broad refactors unless required by the task.

### Package Boundary Rules

- `packages/core/` — zero react-dom, zero localStorage (use StorageAdapter), zero process.env, zero UI libraries. Exception: `core/platform/storage.ts` has an SSR-safe `defaultStorage` using `localStorage` behind `typeof window` guards.
- `packages/ui/` — zero `@multica/core` imports (pure UI, no business logic)
- `packages/views/` — zero `next/*` imports, zero `react-router-dom` imports. Use `NavigationAdapter` for all routing. Use `window.open()` only for external URLs, never for internal navigation.
- `apps/web/platform/` — the only place for Next.js APIs (`next/navigation`)
- `apps/desktop/src/renderer/src/platform/` — the only place for react-router-dom navigation wiring

### Cross-Platform Development Rules

When adding a new page or feature to the shared packages:

1. **New page component** → add to `packages/views/<domain>/`. Import shared components from `@multica/views` and `@multica/ui`. Never import from `next/*` or `react-router-dom`.
2. **Wire it in both apps** → add a route in `apps/web/app/` (Next.js page file) AND in `apps/desktop/src/renderer/src/router.tsx` (react-router route with `handle: { title }`).
3. **Navigation** → use `useNavigation().push()` or `<AppLink>`. Never use `next/link` or react-router's `<Link>` in shared code.
4. **Dynamic page titles** → desktop pages that need dynamic titles (from async data) should use `useDocumentTitle(title)`. Static titles are set automatically via route `handle.title`.
5. **Platform-specific UI** → if a feature is web-only (e.g. SearchCommand) or desktop-only (e.g. TabBar), keep it in the respective app. Use props slots (`extra`, `topSlot`) on shared layout components to inject platform-specific UI.

### CSS Architecture

Both apps share the same CSS foundation. Each app's `globals.css` follows the same import pattern:

```css
@import "tailwindcss";                   /* Core framework */
@import "tw-animate-css";                /* Animation utilities for shadcn */
@import "shadcn/tailwind.css";           /* data-* custom variants + no-scrollbar */
@import "@multica/ui/styles/tokens.css"; /* Design tokens (colors, radius, fonts) */
@import "@multica/ui/styles/base.css";   /* Shared base styles (scrollbar, shiki, body) */
```

- **Shared styles** → `packages/ui/styles/`. Never duplicate scrollbar styling, keyframes, or base layer rules in app CSS.
- **App-specific styles** → keep in the app's own CSS. Web: `apps/web/app/custom.css`. Desktop: inline in `globals.css`.
- **Design tokens** → use semantic tokens (`bg-background`, `text-muted-foreground`, `border-border`). Never use hardcoded Tailwind colors (`text-red-500`, `bg-gray-100`).
- **`@source` directives** → both apps scan `packages/ui/**/*.tsx`, `packages/core/**/*.{ts,tsx}`, `packages/views/**/*.{ts,tsx}` so Tailwind sees all class names used in shared packages.

## UI/UX Rules

- Prefer shadcn components over custom implementations. Install via `npx shadcn add <component> -c apps/web` (monorepo flag required).
- **Shared UI components** → `packages/ui/components/` — shadcn primitives and pure-props common components (multica-icon, theme-provider, actor-avatar, etc.).
- **Shared business components** → `packages/views/<domain>/components/` — pages and domain-bound UI.
- **Web-only components** → `apps/web/features/` or `apps/web/components/`.
- **Desktop-only components** → `apps/desktop/src/renderer/src/components/` (tab-bar, desktop-layout).
- Use shadcn design tokens for styling (e.g. `bg-primary`, `text-muted-foreground`, `text-destructive`). Avoid hardcoded color values (e.g. `text-red-500`, `bg-gray-100`).
- Do not introduce extra state (useState, context, reducers) unless explicitly required by the design.
- Pay close attention to **overflow** (truncate long text, scrollable containers), **alignment**, and **spacing** consistency.
- When unsure about interaction or state design, ask — the user will provide direction.
- **If a component is identical between web and desktop, it belongs in a shared package.** Do not copy-paste between apps.

## Testing Rules

- **TypeScript**: Vitest. Mock external/third-party dependencies only.
- **Go**: Standard `go test`. Tests should create their own fixture data in a test database.

## Commit Rules

- Use atomic commits grouped by logical intent.
- Conventional format:
  - `feat(scope): ...`
  - `fix(scope): ...`
  - `refactor(scope): ...`
  - `docs: ...`
  - `test(scope): ...`
  - `chore(scope): ...`

## CLI Release

**Prerequisite:** A CLI release must accompany every Production deployment. When deploying to Production, always release a new CLI version as part of the process.

1. Create a tag on the `main` branch: `git tag v0.x.x`
2. Push the tag: `git push origin v0.x.x`
3. GitHub Actions automatically triggers `release.yml`: runs Go tests → GoReleaser builds multi-platform binaries → publishes to GitHub Releases + Homebrew tap

By default, bump the patch version each release (e.g. `v0.1.12` → `v0.1.13`), unless the user specifies a specific version.

## Minimum Pre-Push Checks

```bash
make check    # Runs all checks: typecheck, unit tests, Go tests, E2E
```

Run verification only when the user explicitly asks for it.

For targeted checks when requested:
```bash
pnpm typecheck        # TypeScript type errors only
pnpm test             # TS unit tests only (Vitest)
make test             # Go tests only
pnpm exec playwright test   # E2E only (requires backend + frontend running)
```

## AI Agent Verification Loop

After writing or modifying code, always run the full verification pipeline:

```bash
make check
```

This runs all checks in sequence:
1. TypeScript typecheck (`pnpm typecheck`)
2. TypeScript unit tests (`pnpm test`)
3. Go tests (`go test ./...`)
4. E2E tests (auto-starts backend + frontend if needed, runs Playwright)

**Workflow:**
- Write code to satisfy the requirement
- Run `make check`
- If any step fails, read the error output, fix the code, and re-run `make check`
- Repeat until all checks pass
- Only then consider the task complete

**Quick iteration:** If you know only TypeScript or Go is affected, run individual checks first for faster feedback, then finish with a full `make check` before marking work complete.

## E2E Test Patterns

E2E tests should be self-contained. Use the `TestApiClient` fixture for data setup/teardown:

```typescript
import { loginAsDefault, createTestApi } from "./helpers";
import type { TestApiClient } from "./fixtures";

let api: TestApiClient;

test.beforeEach(async ({ page }) => {
  api = await createTestApi();       // logged-in API client
  await loginAsDefault(page);        // browser session
});

test.afterEach(async () => {
  await api.cleanup();               // delete any data created during the test
});

test("example", async ({ page }) => {
  const issue = await api.createIssue("Test Issue");  // create via API
  await page.goto(`/issues/${issue.id}`);             // test via UI
  // api.cleanup() in afterEach removes the issue
});
```
