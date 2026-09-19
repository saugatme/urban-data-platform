"""Generate the submission-ready Week 2 design report PDF."""

from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "docs" / "week2" / "week2_design_report.pdf"


def paragraph(text: str, style: ParagraphStyle):
    return Paragraph(text, style)


def table(rows, widths):
    result = Table(rows, colWidths=widths, repeatRows=1, hAlign="LEFT")
    result.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#17365D")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 7.5),
                ("LEADING", (0, 0), (-1, -1), 9),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#B7C9D6")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#EDF3F7")]),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return result


def add_page_number(canvas, document):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#4F6472"))
    canvas.drawString(2 * cm, 1.25 * cm, "Urban Data Integration Platform | Week 2")
    canvas.drawRightString(A4[0] - 2 * cm, 1.25 * cm, f"Page {document.page}")
    canvas.restoreState()


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    styles = getSampleStyleSheet()
    title = ParagraphStyle(
        "ReportTitle", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=22,
        leading=27, textColor=colors.HexColor("#17365D"), alignment=TA_CENTER, spaceAfter=10,
    )
    subtitle = ParagraphStyle(
        "Subtitle", parent=styles["Normal"], fontSize=11, leading=15, alignment=TA_CENTER,
        textColor=colors.HexColor("#4F6472"), spaceAfter=16,
    )
    heading = ParagraphStyle(
        "Heading", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=13,
        leading=16, textColor=colors.HexColor("#17365D"), spaceBefore=3, spaceAfter=7,
    )
    body = ParagraphStyle(
        "Body", parent=styles["BodyText"], fontSize=9.2, leading=13, spaceAfter=7,
    )
    small = ParagraphStyle("Small", parent=body, fontSize=8.2, leading=11)
    note = ParagraphStyle(
        "Note", parent=small, leftIndent=10, rightIndent=10, textColor=colors.HexColor("#4F6472"),
        spaceBefore=4, spaceAfter=7,
    )

    story = [
        paragraph("Urban Data Integration Platform", title),
        paragraph("Week 2 Design Report — Analytics, Data Products, and Optimisation", subtitle),
        paragraph("<b>Module:</b> Data-intensive Computing &nbsp;&nbsp; <b>Project:</b> NYC Taxi Urban Data Platform", body),
        Spacer(1, 4),
        paragraph("1. Purpose and analytical requirements", heading),
        paragraph(
            "Week 2 turns the canonical Week 1 integrated Delta table into a reusable analytical layer. "
            "The source, <b>data/gold/integrated_taxi_trips</b>, contains 8,480,836 cleaned and enriched trip records. "
            "Week 2 is read-only with respect to that table and answers six stakeholder questions:", body,
        ),
        table([
            [paragraph("ID", small), paragraph("Analytical question", small)],
            [paragraph("Q1", small), paragraph("Which pickup zones have the highest monthly demand?", small)],
            [paragraph("Q2", small), paragraph("How do trip distances vary with weather conditions?", small)],
            [paragraph("Q3", small), paragraph("Does PM2.5 air quality correspond with taxi demand?", small)],
            [paragraph("Q4", small), paragraph("Which zones have the greatest demand variability under different weather conditions?", small)],
            [paragraph("Q5", small), paragraph("What are the busiest pickup hours for each day of the week?", small)],
            [paragraph("Q6", small), paragraph("How does taxi demand change month over month?", small)],
        ], [1.0 * cm, 14.7 * cm]),
        Spacer(1, 9),
        paragraph("2. Analytical query design", heading),
        paragraph(
            "The six analyses are reusable Spark SQL DataFrame functions in <b>src/analytics/queries.py</b>. "
            "Q1 groups by month and pickup zone; Q2 groups distance statistics by normalised weather condition; "
            "Q3 aggregates demand by PM2.5 category; Q4 performs a zone/condition aggregation followed by standard deviation; "
            "Q5 derives weekday and hour from local pickup timestamps; and Q6 calculates monthly totals and month-over-month change. "
            "The 265-row canonical Silver taxi-zone Delta lookup supplies zone names, avoiding a parallel raw-CSV dependency.", body,
        ),
        paragraph(
            "All timestamps use the Week 1 <b>America/New_York</b> Spark session timezone. This makes day-of-week and hour results meaningful for NYC operations rather than dependent on the machine timezone.", note,
        ),
        PageBreak(),
        paragraph("3. Reusable data products", heading),
        paragraph(
            "Four automatically generated Gold-layer Delta products are created by <b>src/analytics/data_products.py</b>. "
            "Every product records data_source, creation_time, refresh_time, and schema_version metadata. "
            "The generator overwrites only these derived tables; it does not change Week 1 Bronze, Silver, or integrated Gold data.", body,
        ),
        table([
            [paragraph("Product", small), paragraph("Primary users", small), paragraph("Decision value", small)],
            [paragraph("daily_mobility", small), paragraph("Transport planners", small), paragraph("Daily zone demand, distance, revenue, and fare trends without a trip-level scan.", small)],
            [paragraph("taxi_zone_statistics", small), paragraph("Operations analysts", small), paragraph("Whole-period comparison of zones for demand, distance, revenue, and fare.", small)],
            [paragraph("weather_impact", small), paragraph("Mobility/weather analysts", small), paragraph("Trip outcomes grouped by temperature bucket and weather condition.", small)],
            [paragraph("air_quality_impact", small), paragraph("Policy analysts", small), paragraph("Trip outcomes grouped by PM2.5 air-quality category.", small)],
        ], [3.4 * cm, 3.3 * cm, 9.0 * cm]),
        Spacer(1, 9),
        paragraph("Storage and refresh trade-off", heading),
        paragraph(
            "The four products occupy <b>1.25 MB</b> against a <b>1.19 GB</b> integrated source: a 0.10% product/source ratio. "
            "This negligible storage overhead is justified by fast access to repeated aggregate questions. Batch overwrite is suitable for the fixed Week 2 dataset; a continuously arriving multi-city feed should instead use incremental Delta MERGE refreshes.", body,
        ),
        paragraph("4. Optimisation strategy", heading),
        paragraph(
            "The optimisation module evaluates caching for repeated aggregation, partition filtering, broadcast joins, and Adaptive Query Execution (AQE). "
            "Each experiment checks that baseline and optimised outputs are identical and prints EXPLAIN FORMATTED output as physical-plan evidence. "
            "Timings are the median of warm runs 2–3; run 1 is excluded because Spark and Delta cold-start work would distort a steady-state comparison.", body,
        ),
        PageBreak(),
        paragraph("5. Measured optimisation results", heading),
        paragraph("Local run context: Spark 3.5.9, Delta Lake 3.2.1, 8,480,836 integrated trips, and a 265-row taxi-zone lookup.", small),
        table([
            [paragraph("Technique", small), paragraph("Baseline", small), paragraph("Optimised", small), paragraph("Change", small), paragraph("Interpretation", small)],
            [paragraph("Cache repeated aggregation", small), paragraph("1.070 s", small), paragraph("0.536 s", small), paragraph("49.91% faster", small), paragraph("Persisted input avoids repeat read and recomputation.", small)],
            [paragraph("year = 2024 filter", small), paragraph("0.861 s", small), paragraph("0.778 s", small), paragraph("9.64% faster", small), paragraph("Plan shows PartitionFilters; most data is already in one year.", small)],
            [paragraph("Broadcast taxi-zone lookup", small), paragraph("3.368 s", small), paragraph("1.244 s", small), paragraph("63.06% faster", small), paragraph("Avoids shuffling 8.48M trips for a 265-row lookup.", small)],
            [paragraph("AQE", small), paragraph("1.187 s", small), paragraph("1.232 s", small), paragraph("3.79% slower", small), paragraph("Already-broadcastable local join leaves little runtime adaptation.", small)],
        ], [3.5 * cm, 2.1 * cm, 2.1 * cm, 2.5 * cm, 6.0 * cm]),
        Spacer(1, 10),
        paragraph("Interpretation", heading),
        paragraph(
            "Broadcast join is the strongest result because the dimension/source size asymmetry is extreme. AQE is not universally harmful: for this small, already well-planned local workload, its planning overhead exceeds its benefit. "
            "It should be re-evaluated with production-size partitions and skewed keys. Partition pruning helps, but its benefit is limited because the available data is primarily one year.", body,
        ),
        paragraph(
            "Q1 and Q4 remain the most demanding analyses. Both scan the full trip table and aggregate many records; Q4 adds a second shuffle for standard deviation across weather-condition groups. "
            "Caching improves repeated access but cannot remove the first-run scan and aggregation cost.", body,
        ),
        PageBreak(),
        paragraph("6. Engineering decisions, scaling, and reproducibility", heading),
        paragraph(
            "Shared paths and weather mappings are centralised in <b>src/analytics/constants.py</b>, preventing query, product, and optimisation code from drifting apart. "
            "<b>src/analytics/runtime.py</b> sets a process-local writable Spark temporary directory before startup. On Windows this avoids a non-fatal cleanup warning caused by inheriting C:\\Windows\\Temp, without changing data locations or query output.", body,
        ),
        paragraph("Scaling to ten cities", heading),
        table([
            [paragraph("Recommendation", small), paragraph("Reason", small)],
            [paragraph("Partition by city", small), paragraph("Allows city filters to skip unrelated files.", small)],
            [paragraph("Review shuffle partitions", small), paragraph("Eight local partitions will under-parallelise a much larger distributed workload.", small)],
            [paragraph("Cluster/Z-order frequent filters", small), paragraph("Improves file locality for city, zone, year, and month predicates.", small)],
            [paragraph("Use incremental Delta MERGE refresh", small), paragraph("Avoids costly full product overwrites as new trips arrive.", small)],
            [paragraph("Run on a real cluster", small), paragraph("Local mode is constrained by one machine's CPU, memory, and disk.", small)],
        ], [5.3 * cm, 10.9 * cm]),
        Spacer(1, 10),
        paragraph("Reproduce the evidence", heading),
        paragraph("1. Run the Week 1 ingestion and integration pipeline.  2. Run <b>python -m src.analytics.data_products</b>.  3. Run <b>python -m src.analytics.optimization --evaluate</b>.  4. Run <b>python run_week2_query_benchmark.py</b> to time all six analytical functions three times and report their warm-run median. The repository README gives the full fresh-clone setup.", body),
        paragraph(
            "Submission contents: source code, analytical notebook, four task reports, this 4-page design report, benchmark methodology/results, and executable setup instructions. Week 1 implementation is not modified by the Week 2 analytics work.", note,
        ),
    ]

    document = SimpleDocTemplate(
        str(OUTPUT), pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm,
        topMargin=1.7 * cm, bottomMargin=1.8 * cm,
        title="Urban Data Integration Platform — Week 2 Design Report",
        author="Group 15",
    )
    document.build(story, onFirstPage=add_page_number, onLaterPages=add_page_number)
    print(OUTPUT)


if __name__ == "__main__":
    main()
