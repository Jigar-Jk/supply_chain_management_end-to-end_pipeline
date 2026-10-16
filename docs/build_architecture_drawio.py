"""
build_architecture_drawio.py – Generate docs/architecture.drawio (the presentation diagram).

Same content as the main Mermaid diagram in docs/architecture.md, laid out for slides:
one column per layer, one row per ingestion pattern, cross-cutting concerns as bands below.
Re-run after changing the architecture:  python docs/build_architecture_drawio.py
"""

from html import escape
from pathlib import Path

OUT = Path(__file__).resolve().parent / "architecture.drawio"

COLORS = {  # fill, stroke
    "src": ("#dae8fc", "#6c8ebf"), "land": ("#e1d5e7", "#9673a6"), "brz": ("#f5d6b4", "#b46504"),
    "gate": ("#fff2cc", "#d6b656"), "quar": ("#f8cecc", "#b85450"), "slv": ("#eeeeee", "#666666"),
    "gld": ("#ffe599", "#bf9000"), "con": ("#d5e8d4", "#82b366"), "band": ("#f5f5f5", "#999999"),
}

# Azure architecture icons (from draw.io's azure2 library), stored in docs/icons/ and EMBEDDED into
# the .drawio file, so they render in every viewer (a library path like img/lib/azure2/... only
# resolves inside the full draw.io app). Source paths in jgraph/drawio: src/main/webapp/img/lib/azure2/
#   globe=general/Globe  file=general/File  sql=databases/SQL_Server  iot=iot/IoT_Hub
#   storage=storage/Storage_Accounts  gear=general/Gear  eventhubs=analytics/Event_Hubs
#   databricks=analytics/Azure_Databricks  dashboard=general/Dashboard
#   ai=ai_machine_learning/Cognitive_Services  powerbi=analytics/Power_BI_Embedded
#   policy=management_governance/Policy  monitor=management_governance/Monitor
#   devops=devops/DevOps_Starter  keyvault=security/Key_Vaults
ICON_DIR = Path(__file__).resolve().parent / "icons"


def _embedded(name: str) -> str:
    """draw.io data-URI image style value: data:image/svg+xml,<base64> (no ';base64' — ';' ends a style key)."""
    import base64
    return "data:image/svg+xml," + base64.b64encode((ICON_DIR / f"{name}.svg").read_bytes()).decode("ascii")
SHAPES = {
    "box": "rounded=1;arcSize=8",
    "cylinder": "shape=cylinder3;boundedLbl=1;backgroundOutline=1;size=9",           # Delta table
    "document": "shape=document;boundedLbl=1;size=0.14",                               # file
    "hexagon": "shape=hexagon;perimeter=hexagonPerimeter2;size=0.08;fixedSize=0",      # quality check
}

cells = []
_next_id = [2]


def _id():
    _next_id[0] += 1
    return f"c{_next_id[0]}"


def icon(name, x, y, size=34):
    style = f"image;aspect=fixed;html=1;points=[];align=center;image={_embedded(name)};"
    cells.append(f'<mxCell id="{_id()}" value="" style="{style}" vertex="1" parent="1">'
                 f'<mxGeometry x="{x}" y="{y}" width="{size}" height="{size}" as="geometry"/></mxCell>')


def box(title, lines, x, y, w, h, kind, shape="box", dashed=False, logo=None):
    """A labelled shape; with `logo`, an Azure icon sits on its left and the text moves right."""
    fill, stroke = COLORS[kind]
    html = f"<b>{escape(title)}</b>" + "".join(f"<br>{escape(l)}" for l in lines)
    style = (f"{SHAPES[shape]};whiteSpace=wrap;html=1;fillColor={fill};strokeColor={stroke};fontSize=12;"
             f"verticalAlign=middle;{'dashed=1;' if dashed else ''}{'spacingLeft=40;' if logo else ''}")
    cid = _id()
    cells.append(f'<mxCell id="{cid}" value="{escape(html)}" style="{style}" vertex="1" parent="1">'
                 f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>')
    if logo:
        icon(logo, x + 8, y + (h - 34) / 2)
    return cid


def lane(title, x, y, w, h, kind, logo=None):
    fill, stroke = COLORS[kind]
    style = (f"swimlane;startSize=30;html=1;fillColor={fill};strokeColor={stroke};fontStyle=1;fontSize=14;"
             "swimlaneFillColor=#ffffff;rounded=1;arcSize=2;")
    cells.append(f'<mxCell id="{_id()}" value="{escape(title)}" style="{style}" vertex="1" parent="1">'
                 f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>')
    if logo:
        icon(logo, x + w - 28, y + 4, size=22)


def arrow(src, dst, label="", dashed=False, color="#444444"):
    style = (f"edgeStyle=orthogonalEdgeStyle;rounded=1;html=1;endArrow=block;endFill=1;strokeColor={color};"
             f"fontSize=11;labelBackgroundColor=#ffffff;{'dashed=1;' if dashed else ''}")
    cells.append(f'<mxCell id="{_id()}" value="{escape(label)}" style="{style}" edge="1" parent="1" '
                 f'source="{src}" target="{dst}"><mxGeometry relative="1" as="geometry"/></mxCell>')


# ------------------------------------------------------------------ title
cells.append(f'<mxCell id="{_id()}" value="{escape("<b>PS-02 Logistics Visibility Control Tower</b><br>Medallion lakehouse on Azure Databricks · four ingestion patterns · governed by Unity Catalog")}" '
             'style="text;html=1;fontSize=20;align=left;verticalAlign=middle;" vertex="1" parent="1">'
             '<mxGeometry x="20" y="10" width="1200" height="50" as="geometry"/></mxCell>')

# ------------------------------------------------------------------ layer lanes
TOP, H = 80, 800
cols = [("Sources", 20, 230, "src"), ("Landing / transport", 270, 240, "land"),
        ("Bronze · Delta, append-only", 530, 270, "brz"), ("Quality gate", 820, 210, "gate"),
        ("Silver · galaxy schema", 1050, 270, "slv"), ("Gold", 1340, 230, "gld"),
        ("Consumption", 1590, 210, "con")]
DATABRICKS_LANES = {"brz", "gate", "slv", "gld"}            # all run on Azure Databricks
for title, x, w, kind in cols:
    lane(title, x, TOP, w, H, kind, logo="databricks" if kind in DATABRICKS_LANES else None)

R = {"api": 130, "batch": 270, "cdc": 410, "stream": 550, "master": 690}   # row tops
BH = 110

# Sources
s_api = box("REST API", ["OpenRouteService", "Open-Meteo weather", "+ carrier shipments"], 35, R["api"], 200, BH, "src", logo="globe")
s_wms = box("Batch file", ["WMS nightly recon", "NDJSON, due 02:00"], 35, R["batch"], 200, BH, "src", shape="document", logo="file")
s_oms = box("CDC source", ["SQL Server OMS", "orders + inventory", "(substitutes Oracle)"], 35, R["cdc"], 200, BH, "src", shape="cylinder", logo="sql")
s_gps = box("Streaming", ["GPS / RFID trucks", "+ pallets", "~2,000 events/s"], 35, R["stream"], 200, BH, "src", logo="iot")
s_md = box("Master data", ["warehouses · products", "customers · carriers"], 35, R["master"], 200, BH, "src", shape="document", logo="file")

# Landing / transport
t_adls = box("ADLS Gen2 landing", ["/landing/<feed>/", "one folder per feed"], 285, R["api"], 210, 250, "land", logo="storage")
t_dbz = box("Debezium", ["reads CDC change tables", "Kafka Connect", "LSN bookmark"], 285, R["cdc"], 210, BH, "land", logo="gear")
t_eh = box("Azure Event Hubs", ["Kafka protocol", "(substitutes Kinesis)", "gps-events · CDC topics"], 285, R["stream"], 210, 130, "land", logo="eventhubs")

# Bronze
b_files = box("bronze.<feed>", ["routes · weather · shipments", "warehouses · products", "customers · carriers",
                                "+ _ingest_timestamp · _source_file · _batch_id"], 545, R["api"], 240, 125, "brz", shape="cylinder")
b_inv = box("bronze.inventory", ["+ WMS SLA check", "ON_TIME · LATE · SLA_MISS"], 545, R["batch"] + 10, 240, 95, "brz", shape="cylinder")
b_cdc = box("bronze.orders_cdc", ["+ inventory_cdc", "op · before · after", "source_position (LSN)"], 545, R["cdc"], 240, BH, "brz", shape="cylinder")
b_gps = box("bronze.gps_events", ["dedup on event_id", "10-minute watermark"], 545, R["stream"], 240, 90, "brz", shape="cylinder")
b_dlq = box("Dead-letter tables", ["gps_events_dlq · cdc_dlq", "unparseable, kept verbatim"], 545, R["master"] - 30, 240, 90, "quar", shape="cylinder")

# Quality gate + quarantine loop
g_gate = box("Quality gate", ["one rule list", "per feed", "keys · ranges · RI", "duplicates", "freshness",
                              "CDC: highest", "LSN wins"], 835, R["api"] + 60, 180, 300, "gate", shape="hexagon")
q_quar = box("quarantine.records", ["PENDING →", "REPROCESSED / REJECTED", "payload verbatim"], 835, R["master"] - 45, 180, 110, "quar", shape="cylinder")
q_rep = box("reprocess job", ["every 2 h", "same transforms", "7-day SLA"], 835, R["master"] + 75, 180, 85, "quar", logo="gear")

# Silver
v_dim = box("6 conformed dimensions", ["dim_warehouse · dim_product", "dim_customer (SCD2)", "dim_carrier · dim_route (SCD1)", "dim_date"], 1065, R["api"], 240, 140, "slv", shape="cylinder")
v_fact = box("3 facts", ["fact_order (CDC, soft deletes)", "fact_shipment (+ last GPS ping)", "fact_inventory_snapshot (is_stale)"], 1065, R["cdc"] - 120, 240, 140, "slv", shape="cylinder")
v_cur = box("inventory_current", ["real-time stock from CDC"], 1065, R["stream"], 240, 80, "slv", shape="cylinder")

# Gold
g_kpi = box("5 KPI views", ["OTIF % · Fill rate %", "Stockout rate %", "Inventory turnover", "Delivery delay risk"], 1355, R["api"], 200, 150, "gld", shape="cylinder")
g_mart = box("KPI mart", ["mart_logistics_kpi_daily", "materialized view", "atomic refresh"], 1355, R["cdc"] - 70, 200, 110, "gld", shape="cylinder")

# Consumption
c_dash = box("AI/BI dashboard", ["KPI tiles", "data-quality tiles"], 1605, R["api"], 180, 110, "con", logo="dashboard")
c_genie = box("Genie space", ["natural language", "→ SQL over Gold"], 1605, R["batch"] + 10, 180, 100, "con", logo="ai")
c_pbi = box("Power BI", ["optional"], 1605, R["cdc"] + 20, 180, 70, "con", dashed=True, logo="powerbi")

# ------------------------------------------------------------------ flows
arrow(s_api, t_adls)
arrow(s_wms, t_adls)
arrow(s_md, t_adls)
arrow(s_oms, t_dbz)
arrow(t_dbz, t_eh)
arrow(s_gps, t_eh)
arrow(t_adls, b_files, "Auto Loader")
arrow(t_adls, b_inv, "Auto Loader")
arrow(t_eh, b_cdc, "Structured Streaming")
arrow(t_eh, b_gps, "checkpoints")
arrow(t_eh, b_dlq, "not JSON", dashed=True, color="#b85450")
for b in (b_files, b_inv, b_cdc, b_gps):
    arrow(b, g_gate)
arrow(g_gate, v_dim, "PASS", color="#2d7d2d")
arrow(g_gate, v_fact, "PASS", color="#2d7d2d")
arrow(g_gate, v_cur, "PASS", color="#2d7d2d")
arrow(g_gate, q_quar, "FAIL", color="#b85450")
arrow(q_quar, q_rep)
arrow(q_rep, v_fact, "healed → MERGE", color="#b85450")
arrow(v_dim, g_kpi)
arrow(v_fact, g_kpi)
arrow(g_kpi, g_mart)
arrow(g_mart, c_dash)
arrow(g_mart, c_genie)
arrow(g_kpi, c_genie)
arrow(g_mart, c_pbi, dashed=True)

# ------------------------------------------------------------------ cross-cutting bands
BAND_Y = TOP + H + 20
box("Governance · Unity Catalog", ["catalog per environment (dev / qa / prod) · schemas bronze, silver, gold, quarantine, ops · "
                                   "group grants · row filter on the mart · column mask on customer_id · tags · column-level lineage"],
    20, BAND_Y, 1780, 60, "band", logo="policy")
box("Observability · ops", ["pipeline_run_log · sla_config · data-quality tiles: volume vs quarantined, quarantine by rule, "
                            "reprocessing success rate, pipeline SLA status (GREEN / AMBER / RED)"],
    20, BAND_Y + 70, 1780, 60, "band", logo="monitor")
box("Infrastructure & delivery (everything as code)", ["Terraform (Azure) · Liquibase (tables, views) · Databricks Asset Bundles (jobs) · "
                                                       "GitHub Actions: lint → tests → validate → DEV → QA → manual approval → PROD · Key Vault (secrets)"],
    20, BAND_Y + 140, 1780, 60, "band", logo="devops")
icon("keyvault", 1755, BAND_Y + 153)

# ------------------------------------------------------------------ legend
LEG_X, LEG_Y = 1290, 12
cells.append(f'<mxCell id="{_id()}" value="{escape("<b>Legend</b>")}" style="text;html=1;fontSize=12;align=left;" '
             f'vertex="1" parent="1"><mxGeometry x="{LEG_X}" y="{LEG_Y}" width="60" height="20" as="geometry"/></mxCell>')
for i, (shape, label) in enumerate([("cylinder", "Delta table"), ("document", "file"),
                                     ("hexagon", "quality check"), ("box", "service / job")]):
    box(label, [], LEG_X + 60 + i * 115, LEG_Y, 105, 46, "band", shape=shape)

xml = (
    '<mxfile host="app.diagrams.net" type="device">'
    '<diagram id="architecture" name="Architecture">'
    '<mxGraphModel dx="1800" dy="1100" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" '
    'fold="1" page="1" pageScale="1" pageWidth="1820" pageHeight="1140" math="0" shadow="0">'
    '<root><mxCell id="0"/><mxCell id="1" parent="0"/>' + "".join(cells) + "</root></mxGraphModel></diagram></mxfile>"
)
OUT.write_text(xml, encoding="utf-8")
print(f"Wrote {OUT} ({len(cells)} cells)")
