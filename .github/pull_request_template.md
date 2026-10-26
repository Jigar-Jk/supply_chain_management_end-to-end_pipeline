## What and why

<!-- What does this change do, and why is it needed? One or two sentences. -->

## How it was tested

- [ ] `python -m ruff check .` passes
- [ ] `python -m unittest discover -s tests -t .` passes locally (CI runs it again)
- [ ] New behaviour has tests, including a case that would fail without the change

## Checklist

- [ ] No secrets, keys, tokens or connection strings in the diff (they belong in environment variables / Key Vault)
- [ ] Table changes are a **new** Liquibase changeset, never an edit of an applied one
- [ ] `infra/` changed: `terraform validate` passes and the `terraform plan` was reviewed
- [ ] `databricks.yml` / `databricks/` changed: `databricks bundle validate` shows no unknown fields
- [ ] Docs / README updated if behaviour or usage changed
- [ ] Tried on Azure? Say so, and confirm the stack was destroyed afterwards (`scripts/down.ps1`)
