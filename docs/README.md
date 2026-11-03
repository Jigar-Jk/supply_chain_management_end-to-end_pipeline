# Documentation

Start with the project guide; the other pages go deeper into one topic each.

| Page | What it covers |
|---|---|
| [project_guide.md](project_guide.md) | The whole project part by part: sources, layers, data quality, schema management, KPIs, governance, infrastructure, CI/CD, workflows, status and likely questions |
| [architecture.md](architecture.md) | Architecture diagrams: main flow, quarantine lifecycle, CDC path, deployment |
| [data_model.md](data_model.md) | ER diagrams and table notes for the Silver galaxy schema and the ops tables |
| [cdc.md](cdc.md) | CDC runbook: SQL Server → Debezium → Bronze → Silver, failure scenarios, local demo |
| [decisions.md](decisions.md) | Architecture decision records |

## Diagrams

Editable diagrams (draw.io) and their exports live next to the pages that use them:
[full_architecture.drawio](full_architecture.drawio) (the main architecture diagram),
[data_model.drawio](data_model.drawio), `data_model_galaxy.png`, `data_model_ops.png`, and the
icons in [icons/](icons/).

## Archive

[archive/](archive/) holds the first set of notes written while the platform was being built (overview,
architecture, implementation log, infrastructure, planned data flows). They are kept for history and
are not maintained; where they disagree with the pages above, the pages above are correct.
