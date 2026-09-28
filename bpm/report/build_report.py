# -*- coding: utf-8 -*-
import os
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image,
    PageBreak, Preformatted, KeepTogether, HRFlowable
)

BASE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT = os.path.join(BASE, "bpm", "BPM_Case_Study_Report.pdf")
BPMN_IMG = os.path.join(BASE, "bpm", "report", "assets", "bpmn_diagram.png")

styles = getSampleStyleSheet()

styles.add(ParagraphStyle(name="TitleBig", fontSize=20, leading=26, alignment=TA_CENTER,
                           fontName="Helvetica-Bold", spaceAfter=6, textColor=colors.HexColor("#1a3a5c")))
styles.add(ParagraphStyle(name="TitleSub", fontSize=13, leading=18, alignment=TA_CENTER,
                           fontName="Helvetica", spaceAfter=4, textColor=colors.HexColor("#444444")))
styles.add(ParagraphStyle(name="TitleMeta", fontSize=11, leading=16, alignment=TA_CENTER,
                           fontName="Helvetica", textColor=colors.HexColor("#666666")))
styles.add(ParagraphStyle(name="H1", fontSize=15, leading=19, spaceBefore=14, spaceAfter=8,
                           fontName="Helvetica-Bold", textColor=colors.HexColor("#1a3a5c")))
styles.add(ParagraphStyle(name="H2", fontSize=12.5, leading=16, spaceBefore=10, spaceAfter=6,
                           fontName="Helvetica-Bold", textColor=colors.HexColor("#2a5d8f")))
styles.add(ParagraphStyle(name="Body", fontSize=9.7, leading=14, spaceAfter=6, fontName="Helvetica"))
styles.add(ParagraphStyle(name="BodyItalic", fontSize=9.5, leading=13.5, spaceAfter=6,
                           fontName="Helvetica-Oblique", textColor=colors.HexColor("#555555")))
styles.add(ParagraphStyle(name="CodeBlock", fontSize=7.4, leading=9.2, fontName="Courier",
                           backColor=colors.HexColor("#f5f5f5"), borderPadding=6,
                           leftIndent=2, rightIndent=2))
styles.add(ParagraphStyle(name="Caption", fontSize=8.5, leading=11, alignment=TA_CENTER,
                           fontName="Helvetica-Oblique", textColor=colors.HexColor("#666666"), spaceBefore=4, spaceAfter=10))
styles.add(ParagraphStyle(name="TableCell", fontSize=7.5, leading=9.4, fontName="Helvetica"))
styles.add(ParagraphStyle(name="TableCellBold", fontSize=7.5, leading=9.4, fontName="Helvetica-Bold"))
styles.add(ParagraphStyle(name="TableHead", fontSize=7.7, leading=9.8, fontName="Helvetica-Bold", textColor=colors.white))

def P(text, style="Body"):
    return Paragraph(text, styles[style])

def cell(text, bold=False):
    return Paragraph(text, styles["TableCellBold"] if bold else styles["TableCell"])

def make_table(data, col_widths, header=True, zebra=True):
    rows = []
    for i, row in enumerate(data):
        if header and i == 0:
            rows.append([Paragraph(str(c), styles["TableHead"]) for c in row])
        else:
            rows.append([c if hasattr(c, "wrap") else cell(str(c)) for c in row])
    t = Table(rows, colWidths=col_widths, repeatRows=1 if header else 0)
    style = [
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]
    if header:
        style.append(("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2a5d8f")))
    if zebra:
        for r in range(1 if header else 0, len(rows)):
            if (r - (1 if header else 0)) % 2 == 1:
                style.append(("BACKGROUND", (0, r), (-1, r), colors.HexColor("#f2f6fa")))
    t.setStyle(TableStyle(style))
    return t

def code_block(text):
    return Preformatted(text, styles["CodeBlock"])

story = []

# ===================== TITLE PAGE =====================
story.append(Spacer(1, 2.2 * cm))
story.append(P("22MDCE01 &mdash; Business Process Management", "TitleSub"))
story.append(P("Cycle II &ndash; Project Based Case Study", "TitleSub"))
story.append(Spacer(1, 1.0 * cm))
story.append(P("Real-Time Process Mining for Quick-Commerce", "TitleBig"))
story.append(P("Order Fulfillment &amp; Dark-Store Dispatch", "TitleBig"))
story.append(Spacer(1, 0.6 * cm))
story.append(P("A Kafka &rarr; PySpark Structured Streaming &rarr; Delta Lake case study applying real-time"
               " BPM process mining, SLA monitoring, and anomaly detection to a simulated dark-store"
               " fulfillment process.", "TitleSub"))
story.append(Spacer(1, 2.0 * cm))
story.append(HRFlowable(width="60%", thickness=1, color=colors.HexColor("#cccccc"), hAlign="CENTER"))
story.append(Spacer(1, 0.4 * cm))
story.append(P("Name: ______________________________", "TitleMeta"))
story.append(P("Register No: ______________________________", "TitleMeta"))
story.append(P("Class / Section: ______________________________", "TitleMeta"))
story.append(Spacer(1, 1.2 * cm))
story.append(P("Project repository: github.com/itzbhav/realtime-lakehouse-pipeline (folder: bpm/)", "TitleMeta"))
story.append(PageBreak())

# ===================== 1. BUSINESS CONTEXT =====================
story.append(P("1. Business Context", "H1"))
story.append(P(
    "A quick-commerce dark store (Blinkit / Instamart style) operates under a strict 15-minute delivery "
    "promise. An order moves through picking, bagging, rider dispatch, and doorstep delivery; a delay or "
    "process failure at any single stage &mdash; a stuck picker, an undocumented item substitution, a rider "
    "handoff that never happens &mdash; directly breaks the SLA and costs the business a refund or a lost "
    "customer. This case study builds the real-time pipeline that would catch those failures "
    "<b>as they happen</b>, not in a post-mortem the next morning.", "Body"))
story.append(P(
    "The pipeline reuses the same Docker-based lakehouse infrastructure (Kafka, Spark Structured Streaming, "
    "Delta Lake on MinIO, Hive Metastore) built for the Data Engineering Lab project, applied here to a new "
    "BPM-shaped event domain corresponding to Scenario 4 (Superstore / Quick-Commerce) of the assignment brief.",
    "Body"))

story.append(P("2. Process Stages", "H1"))
story.append(P("The order fulfillment process consists of six sequential stages, one exception activity, "
               "and one alternate terminal (cancellation) path:", "Body"))
story.append(make_table(
    [["#", "Activity", "Swimlane", "Description"],
     ["0", "ORDER_RECEIVED", "Customer App", "Customer places the order; case begins"],
     ["1", "PICKER_ASSIGNED", "Dark Store Ops", "A store picker is assigned to fulfil the order"],
     ["\u2014", "ITEM_SUBSTITUTED", "Dark Store Ops (exception)", "Out-of-stock item replaced; does not advance the main sequence"],
     ["2", "ITEMS_BAGGED", "Dark Store Ops", "All items packed and ready for handoff"],
     ["3", "RIDER_ASSIGNED", "Rider / Courier", "A delivery rider is assigned (picker\u2192rider handoff)"],
     ["4", "DISPATCHED_FROM_DARKSTORE", "Rider / Courier", "Rider leaves the dark store with the order"],
     ["5", "DOORSTEP_DELIVERED", "Rider / Courier", "Order handed to the customer (rider\u2192doorstep handoff)"],
     ["\u2014", "ORDER_CANCELLED", "Exception Handling", "Alternate terminal: picker/rider unavailable or timeout"],
     ],
    col_widths=[0.9 * cm, 5.6 * cm, 2.9 * cm, 7.0 * cm]))
story.append(Spacer(1, 6))

# ===================== 3. EVENT TAXONOMY =====================
story.append(P("3. Event Taxonomy", "H1"))
story.append(P("Every process event follows the case_id / activity / timestamp taxonomy required by the "
               "assignment, with resource, location, and a metadata payload:", "Body"))
story.append(code_block(
'''{
  "case_id": "ORD-SUPER-000047",
  "activity": "ITEMS_BAGGED",
  "timestamp": "2026-09-28T11:03:40Z",
  "resource": "Picker_Emp_112",
  "location": "DarkStore_Coimbatore_North",
  "metadata": {
    "item_count": 8,
    "target_total_minutes": 15,
    "picking_sla_minutes": 3
  }
}'''))
story.append(P("<b>ITEM_SUBSTITUTED</b> events carry two additional metadata fields "
               "(<font face='Courier'>substituted_item</font>, <font face='Courier'>reason</font>) and are "
               "logged as exception events that do not advance the case's main sequence position.", "Body"))

# ===================== 4. BPMN =====================
story.append(PageBreak())
story.append(P("4. BPMN Process Map", "H1"))
story.append(Image(BPMN_IMG, width=17.2 * cm, height=17.2 * cm * (2044 / 3057)))
story.append(P("Figure 1. Three swimlanes (Customer App / Dark Store Ops / Rider), one exclusive gateway "
               "for the out-of-stock substitution exception, two courier-handoff points (picker&rarr;rider "
               "at ITEMS_BAGGED&rarr;RIDER_ASSIGNED, rider&rarr;doorstep at "
               "DISPATCHED_FROM_DARKSTORE&rarr;DOORSTEP_DELIVERED), and the cancellation escape path.",
               "Caption"))

# ===================== 5. KPIs =====================
story.append(P("5. Key Performance Indicators", "H1"))
story.append(make_table(
    [["KPI", "Definition", "Computed in"],
     ["Cycle time", "Minutes between consecutive stage events (stage_minutes); total ORDER_RECEIVED\u2192DOORSTEP_DELIVERED time (total_minutes)", "bpm_process_engine.py, per event"],
     ["SLA limits", "Per-stage targets: 1 / 3 / 2 / 2 / 7 min, summing to the 15-min target_total_minutes promise. Picking stage is data-driven from metadata.picking_sla_minutes; others are fixed ops constants.", "STAGE_SLA_MINUTES constant"],
     ["Bottleneck stage", "The stage-to-stage transition with the highest average stage_minutes or highest SLA-breach rate", "Printed every batch; queryable via bpm.process_events"],
     ["Throughput", "Count of DOORSTEP_DELIVERED events per micro-batch / time window", "Printed as part of the batch KPI line"],
     ["Drop-off rate", "% of cases whose latest known state (bpm.case_state) never reached DOORSTEP_DELIVERED", "Query against case_state"],
     ],
    col_widths=[2.6 * cm, 10.6 * cm, 3.4 * cm]))

story.append(PageBreak())

# ===================== 6. ANOMALY ENGINE =====================
story.append(P("6. Real-Time Process Analytics &amp; Anomaly Detection Engine", "H1"))
story.append(P("<b>(Rubric component 2 &mdash; 15 marks)</b>", "BodyItalic"))
story.append(P(
    "<font face='Courier'>bpm_process_engine.py</font> is a PySpark Structured Streaming job that reads "
    "<font face='Courier'>order_fulfillment_events</font> from Kafka and, per micro-batch, reconstructs each "
    "case's process state: first via an in-batch window function (since a fast-moving case can have several "
    "events land in the same 10-second trigger), falling back to a persisted <font face='Courier'>case_state</font> "
    "Delta table &mdash; upserted via a Delta <font face='Courier'>MERGE</font> &mdash; for state carried across "
    "batches. Every event, anomalous or not, is written to <font face='Courier'>bpm.process_events</font> as an "
    "immutable audit log with the flags below attached, so nothing is silently dropped.", "Body"))

story.append(P("6.1 Anomaly Detection Rules", "H2"))
story.append(make_table(
    [["Anomaly", "Detection rule"],
     ["SLA breach", "stage_minutes &gt; sla_limit_minutes for that transition, or (at delivery) total_minutes &gt; metadata.target_total_minutes"],
     ["Duplicate task", "Incoming activity equals the case's previous activity"],
     ["Out-of-sequence activity", "Incoming stage index \u2264 the case's previous stage index (went backward or repeated out of place), or the case's very first observed event is not ORDER_RECEIVED"],
     ["Process deviation", "Surfaces as a combination of the above \u2014 e.g. DOORSTEP_DELIVERED with no prior DISPATCHED_FROM_DARKSTORE shows up as an out-of-sequence flag on the delivery event"],
     ],
    col_widths=[3.6 * cm, 13.0 * cm]))

story.append(P("6.2 Verified Run &mdash; Injected vs. Detected Anomalies", "H2"))
story.append(P(
    "The producer (<font face='Courier'>bpm_producer.py</font>) simulated 40 orders, deliberately injecting "
    "anomalies at fixed rates, then <font face='Courier'>bpm_process_engine.py</font> was run against the "
    "resulting stream. Detected counts are compared against what the producer actually injected:", "Body"))
story.append(make_table(
    [["Anomaly type", "Injected by producer", "Detected by engine", "Match"],
     ["SLA breach (cases)", "4", "4 cases \u00d7 2 flagged rows each (stage + cascading total) = 8", "\u2713 exact"],
     ["Duplicate task", "2", "2", "\u2713 exact"],
     ["Out-of-sequence", "1", "1", "\u2713 exact"],
     ["Substitution", "7", "7", "\u2713 exact"],
     ["Drop-off (never delivered)", "1", "1 (stuck at RIDER_ASSIGNED in case_state)", "\u2713 exact"],
     ["Total events processed", "\u2014", "247", "\u2014"],
     ["Orders delivered (throughput)", "39 / 40", "39", "\u2713 exact"],
     ],
    col_widths=[4.6 * cm, 4.0 * cm, 6.2 * cm, 1.8 * cm]))
story.append(P("Console output from the live run:", "Body"))
story.append(code_block(
""">>> RUNNING TOTALS \u2014 events: 247 | delivered: 39 | sla_breaches: 8 | duplicates: 2 |
    out_of_sequence: 1 | substitutions: 7"""))

story.append(P("6.3 SLA-Breach Detail (queried from bpm.process_events)", "H2"))
story.append(P(
    "All four deliberately-breached cases show exactly the expected pattern: the specific stage they were "
    "designed to blow, plus a cascading total-time breach at delivery (realistic \u2014 a late stage naturally "
    "makes the whole order late):", "Body"))
story.append(make_table(
    [["case_id", "activity", "stage_min", "sla_limit", "sla_breached", "total_min", "total_breached"],
     ["ORD-SUPER-000001", "PICKER_ASSIGNED", "3.47", "1.0", "true", "3.47", "false"],
     ["ORD-SUPER-000001", "DOORSTEP_DELIVERED", "6.92", "7.0", "false", "16.97", "true"],
     ["ORD-SUPER-000003", "PICKER_ASSIGNED", "3.93", "1.0", "true", "3.93", "false"],
     ["ORD-SUPER-000003", "DOORSTEP_DELIVERED", "6.92", "7.0", "false", "17.27", "true"],
     ["ORD-SUPER-000019", "ITEMS_BAGGED", "6.40", "3.0", "true", "7.33", "false"],
     ["ORD-SUPER-000019", "DOORSTEP_DELIVERED", "6.73", "7.0", "false", "17.82", "true"],
     ["ORD-SUPER-000030", "RIDER_ASSIGNED", "5.07", "2.0", "true", "8.73", "false"],
     ["ORD-SUPER-000030", "DOORSTEP_DELIVERED", "6.90", "7.0", "false", "17.33", "true"],
     ],
    col_widths=[3.3 * cm, 3.7 * cm, 1.9 * cm, 1.5 * cm, 2.3 * cm, 1.6 * cm, 2.3 * cm]))

story.append(P("6.4 Duplicate &amp; Out-of-Sequence Detail", "H2"))
story.append(make_table(
    [["case_id", "activity", "prev_activity", "is_duplicate", "is_out_of_sequence"],
     ["ORD-SUPER-000007", "PICKER_ASSIGNED", "ITEMS_BAGGED", "false", "true"],
     ["ORD-SUPER-000017", "DISPATCHED_FROM_DARKSTORE", "DISPATCHED_FROM_DARKSTORE", "true", "false"],
     ["ORD-SUPER-000021", "DISPATCHED_FROM_DARKSTORE", "DISPATCHED_FROM_DARKSTORE", "true", "false"],
     ],
    col_widths=[3.1 * cm, 4.6 * cm, 4.6 * cm, 2.0 * cm, 2.9 * cm]))
story.append(P(
    "Case 000007's out-of-sequence swap is visible one level deeper too: its <font face='Courier'>PICKER_"
    "ASSIGNED</font> and <font face='Courier'>ITEMS_BAGGED</font> events had their timestamps deliberately "
    "swapped, so the engine correctly sees ITEMS_BAGGED arriving (by event time) before PICKER_ASSIGNED for "
    "that case &mdash; exactly the process-order violation the detector is designed to catch.", "Body"))

story.append(PageBreak())
story.append(P("6.5 Stage Duration / Bottleneck Analysis", "H2"))
story.append(P("Real output from the live run (average minutes per stage transition):", "Body"))
story.append(make_table(
    [["prev_activity", "activity", "avg_minutes", "n"],
     ["DISPATCHED_FROM_DARKSTORE", "DOORSTEP_DELIVERED", "6.83", "39"],
     ["PICKER_ASSIGNED", "ITEMS_BAGGED", "2.91", "39"],
     ["ITEMS_BAGGED", "RIDER_ASSIGNED", "1.92", "39"],
     ["RIDER_ASSIGNED", "DISPATCHED_FROM_DARKSTORE", "1.85", "39"],
     ["ORDER_RECEIVED", "PICKER_ASSIGNED", "0.98", "39"],
     ["ITEMS_BAGGED", "PICKER_ASSIGNED *", "2.80", "1"],
     ["PICKER_ASSIGNED", "RIDER_ASSIGNED *", "1.87", "1"],
     ["ORDER_RECEIVED", "ITEMS_BAGGED *", "0.87", "1"],
     ["DISPATCHED_FROM_DARKSTORE", "DISPATCHED_FROM_DARKSTORE *", "0.29", "2"],
     ],
    col_widths=[5.6 * cm, 5.6 * cm, 3.0 * cm, 2.4 * cm]))
story.append(P("<font face='Courier'>*</font> rows are direct evidence of injected anomalies: the "
               "out-of-sequence case (000007) disturbs three neighbouring transition pairs in the audit log "
               "(its own swap plus both adjacent pairings), and the two duplicate DISPATCHED_FROM_DARKSTORE "
               "events show up as a same-activity transition with a near-zero gap.", "Body"))
story.append(P("<b>Bottleneck identified:</b> DISPATCHED_FROM_DARKSTORE \u2192 DOORSTEP_DELIVERED, at 6.83 "
               "minutes average against a 7-minute SLA &mdash; the transit leg consumes the largest share of "
               "the 15-minute delivery promise and is the stage with the least slack.", "Body"))

# ===================== 7. QUERYING =====================
story.append(PageBreak())
story.append(P("7. Querying the Lakehouse for Process Metrics", "H1"))
story.append(P("<b>(Rubric component 3 &mdash; 15 marks)</b>", "BodyItalic"))

story.append(P("7.1 Working today &mdash; via spark-sql", "H2"))
story.append(P(
    "Both <font face='Courier'>bpm.process_events</font> (the immutable fact log) and "
    "<font face='Courier'>bpm.case_state</font> (latest per-case snapshot) are registered in Hive Metastore "
    "and queryable by name today:", "Body"))
story.append(code_block(
"""SELECT last_activity, count(*)
FROM bpm.case_state
GROUP BY last_activity
ORDER BY 2 DESC;

-- Verified result:
-- DOORSTEP_DELIVERED   39
-- RIDER_ASSIGNED        1"""))
story.append(P(
    "This is the &ldquo;active bottleneck / drop-off&rdquo; query the assignment asks for: it shows exactly "
    "where in-flight and abandoned orders currently sit, with the one drop-off case correctly stuck at "
    "RIDER_ASSIGNED, matching the producer's own injected drop-off exactly.", "Body"))

story.append(P("7.2 Querying via Trino (pending Week 3 infrastructure)", "H2"))
story.append(P(
    "Trino is not yet in the project's <font face='Courier'>docker-compose.yml</font> &mdash; that is a "
    "separate, later phase of the Data Engineering Lab project. Once added with a catalog pointing at this "
    "same Hive Metastore, the following queries are ready to run for this rubric section, written against "
    "the live schema above:", "Body"))
story.append(code_block(
"""-- Order Turnaround Time (TAT) per dark store
SELECT location,
       round(avg(total_minutes), 2) AS avg_tat_minutes,
       round(approx_percentile(total_minutes, 0.95), 2) AS p95_tat_minutes
FROM bpm.process_events
WHERE activity = 'DOORSTEP_DELIVERED'
GROUP BY location
ORDER BY avg_tat_minutes DESC;

-- Stage-by-stage bottleneck heatmap
SELECT prev_activity, activity,
       round(avg(stage_minutes), 2) AS avg_minutes,
       round(100.0 * sum(CASE WHEN sla_breached THEN 1 ELSE 0 END) / count(*), 1) AS sla_breach_pct
FROM bpm.process_events
WHERE stage_minutes IS NOT NULL
GROUP BY prev_activity, activity
ORDER BY sla_breach_pct DESC;

-- Drop-off rate: cases stuck / never delivered
SELECT last_activity, count(*) AS stuck_cases
FROM bpm.case_state
WHERE last_activity <> 'DOORSTEP_DELIVERED'
GROUP BY last_activity
ORDER BY stuck_cases DESC;

-- Active bottleneck: cases currently mid-process, by current stage
SELECT last_activity AS current_stage, count(*) AS cases_in_stage
FROM bpm.case_state
GROUP BY last_activity
ORDER BY cases_in_stage DESC;"""))

# ===================== 8. RUN STEPS =====================
story.append(PageBreak())
story.append(P("8. Reproducing These Results", "H1"))
story.append(P("Same Docker stack as the main Data Engineering Lab project &mdash; no new services needed "
               "for the producer/engine.", "Body"))
story.append(code_block(
"""# 1. Stack already running (docker compose up -d)

# 2. Simulate orders (host terminal)
python bpm/bpm_producer.py --cases 40

# 3. Run the process engine (separate terminal)
docker compose exec spark spark-submit \\
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,\\
io.delta:delta-spark_2.12:3.3.3,org.apache.hadoop:hadoop-aws:3.3.4 \\
  bpm/bpm_process_engine.py"""))

# ===================== 9. RUBRIC MAPPING =====================
story.append(P("9. Rubric Mapping (40 Marks)", "H1"))
story.append(make_table(
    [["Component", "Marks", "Evidence in this report"],
     ["BPMN diagram, process stages, KPIs, event taxonomy", "10",
      "Sections 2\u20135 \u2014 BPMN diagram (Fig. 1), stage table, event schema, KPI table"],
     ["PySpark SLA &amp; anomaly detection", "15",
      "Section 6 \u2014 engine architecture, detection rules, and verified live-run results matching every injected anomaly exactly"],
     ["Querying the lakehouse for process metrics", "15",
      "Section 7 \u2014 working spark-sql query with verified result today; Trino queries drafted and ready for Week 3"],
     ],
    col_widths=[9.6 * cm, 1.6 * cm, 5.4 * cm]))

# ===================== APPENDICES =====================
story.append(PageBreak())
story.append(P("Appendix A: bpm_producer.py", "H1"))
with open(os.path.join(BASE, "bpm", "bpm_producer.py"), "r", encoding="utf-8") as f:
    story.append(code_block(f.read()))

story.append(PageBreak())
story.append(P("Appendix B: bpm_process_engine.py", "H1"))
with open(os.path.join(BASE, "bpm", "bpm_process_engine.py"), "r", encoding="utf-8") as f:
    story.append(code_block(f.read()))

# ===================== BUILD =====================
def add_page_number(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#888888"))
    canvas.drawCentredString(A4[0] / 2, 1.2 * cm, f"Page {doc.page}")
    canvas.restoreState()

doc = SimpleDocTemplate(OUT, pagesize=A4,
                         leftMargin=1.8 * cm, rightMargin=1.8 * cm,
                         topMargin=1.8 * cm, bottomMargin=1.8 * cm,
                         title="BPM Case Study: Quick-Commerce Order Fulfillment",
                         author="Data Engineering Lab / BPM Cycle II")
doc.build(story, onFirstPage=add_page_number, onLaterPages=add_page_number)
print("PDF written to", OUT)
