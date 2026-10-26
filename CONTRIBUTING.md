# Contributing

How changes are made in this repository. `main` is protected: every change goes through a pull
request, and a pull request can only be merged when CI (lint, all tests, wheel build) is green.

## Workflow

```powershell
git checkout main
git pull
git checkout -b feat/short-description      # one branch per change
# edit, commit
git push -u origin feat/short-description
```

Then open a pull request on GitHub, fill in the template, wait for **CI / test** to pass, and merge
with **Squash and merge**. Delete the branch afterwards, locally too:

```powershell
git checkout main
git pull
git branch -d feat/short-description
```

## Branch names

| Prefix | For |
|---|---|
| `feat/` | New behaviour (a feature, a quality rule, a KPI) |
| `fix/` | Bug fix |
| `infra/` | Terraform, bundle, deployment scripts |
| `test/` | Tests only |
| `docs/` | Documentation only |
| `ci/` | GitHub Actions workflows |
| `chore/` | Repository housekeeping |

## Commit messages

`<type>: <what changed>` with the same types as the branch prefixes, for example
`feat: schema drift detection` or `fix: Bronze waits for every stream`. Explain the *why* in the body
when it is not obvious.

## Before opening a pull request

```powershell
python -m ruff check .
python -m unittest discover -s tests -t .
```

Changes under `infra/`: also `terraform -chdir=infra validate`. Changes to the bundle: also
`databricks bundle validate` (without a workspace, only the login error is expected).

## Rules

- **Secrets never go into git.** Keys, tokens and passwords come from environment variables
  (`ORS_API_KEY`, `DEBEZIUM_SQL_PASSWORD`, `ARM_SUBSCRIPTION_ID`) or Key Vault. Push protection is on.
- **Applied Liquibase changesets are never edited**: Liquibase checksums them, and an edit breaks
  every database that already ran them. Add a new changeset instead.
- **New behaviour comes with tests**, including one that fails without the change.
- **The pipeline never guesses about data**: unexpected input is quarantined with its cause, not
  silently fixed (see `COLUMN_ALIASES` for agreed column renames).
- **Azure costs money while it exists.** Start a session with `scripts/up.ps1` and always end it
  with `scripts/down.ps1`.

## Ownership

`.github/CODEOWNERS` lists who reviews which area; GitHub requests their review automatically.
