# CLAUDE.md — littledevil-recorder

Full architecture: `docs/` (submodule → littledevil-docs, pinned; `git submodule update --remote docs` to refresh).

## This repo's scope
Data Floor — trade/depth ingestion, Universe Refresh, Event Gates, Data Health. The one service that must never stop. Copilot "Backend Service", public subnet, outbound-only security group — no NAT Gateway.

Primary references: docs/orchestration-and-platform.md §2; docs/architecture-review.md §2, §6.1, §8b (cold-start/backfill); docs/data-and-events.md §2.

## Non-negotiables (repeated here so they hold even if docs/ isn't checked out)
- Agents request; Python executes and gates.
- The RR floor, risk limits, fill rule, and forbidden-claims/forbidden-fields lists are speed-bumped — no silent path around them (docs/orchestration-and-platform.md §12).
- Never fit a detector parameter against the same data used to validate it — dev/holdout split, always (docs/architecture-review.md §8, §8b).
- A frozen decision is never edited after the fact — corrections are new rows.
- Recording never pauses, for any reason.
- `README.md`'s ~37x/year aspiration is never an input to any agent, prompt, config, or task — see the workspace-root `CLAUDE.md`'s non-negotiables for the full statement of this rule.
- The Vercel↔AWS boundary is never trusted by network origin, only by the minted token (docs/repo-structure.md §5).
- No paid data sources in v1.
- Update the workspace-root `DEV_JOURNAL.md` before every final response — never create or use a repo-local journal.

If a task seems to need something not covered by any of the above, that's a signal to stop and ask, not to invent it.
