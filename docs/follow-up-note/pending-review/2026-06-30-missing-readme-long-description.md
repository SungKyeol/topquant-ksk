# sdist build warns no README — PyPI page has no long description

**Date raised**: 2026-06-30
**Source**: Build warning during PyPI release of 0.1.25 (`python -m build`)
**Status**: Deferred

## Motivation

Every build emits:

```
warning: sdist: standard file not found: should have one of README, README.rst, README.txt, README.md
```

The repo has no README and `pyproject.toml` has no `readme = ` field, so:

- The PyPI project page (https://pypi.org/project/topquant-ksk/) shows **no long
  description** — just the one-line `description`. Poor first impression for anyone
  evaluating `pip install topquant-ksk`.
- The warning is pre-existing (present across all 0.1.x releases), not introduced by
  any recent change. Non-blocking, but persistent noise on every build.

## Proposed approach

1. Add `README.md` at the repo root (overview, install `pip install topquant-ksk[all]`,
   minimal usage — can reuse content from CLAUDE.md / the QuantDB example, minus the
   hardcoded credentials).
2. Add `readme = "README.md"` under `[project]` in `pyproject.toml`.
3. Verify: `python -m build` no longer warns about the missing README, and the PyPI
   page renders the long description on the next release.

## Non-goals

- Not blocking any release; current packages install fine without it.
- Initial README can be minimal and grow later — goal is to clear the warning and give
  the PyPI page a real description.
