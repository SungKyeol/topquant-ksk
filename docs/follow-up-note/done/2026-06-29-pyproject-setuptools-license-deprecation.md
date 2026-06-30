# pyproject license metadata uses deprecated setuptools forms

**Date raised**: 2026-06-29
**Source**: Build output during PyPI release of 0.1.24 (`python -m build`)
**Status**: Done
**Closed**: 2026-06-30
**Resolution**: Migrated pyproject to SPDX `license = "MIT"` + `license-files = ["LICENSE"]`, removed deprecated MIT classifier, bumped `setuptools>=77.0` (commit 2f3bc36). Shipped in 0.1.25 on PyPI — metadata `License-Expression: MIT` verified; build emits no license deprecation warnings.

## Motivation

The 0.1.24 build emitted `SetuptoolsDeprecationWarning` for two `pyproject.toml`
license declarations:

1. `[project] license = { file="LICENSE" }` — TOML-table form is deprecated.
2. `License :: OSI Approved :: MIT License` in `classifiers` — license classifiers
   are deprecated.

setuptools states: **"By 2027-Feb-18, you need to update your project and remove
deprecated calls or your builds will no longer be supported."** So this is a
hard future-breaking issue, not cosmetic — once the local setuptools crosses that
cutoff, `python -m build` will fail.

## Proposed approach

Migrate to the SPDX string form (requires setuptools>=77.0.0):

- Replace `license = { file="LICENSE" }` with `license = "MIT"`.
- Add `license-files = ["LICENSE"]` under `[project]`.
- Remove `"License :: OSI Approved :: MIT License"` from `classifiers`.
- Bump the build-system requirement to `requires = ["setuptools>=77.0"]`.

Verify a clean `python -m build` with no license deprecation warnings, then ship
on the next release.

## Non-goals

- No change to the actual license (stays MIT).
- Not blocking any release before the 2027-Feb-18 cutoff; current builds still work.
