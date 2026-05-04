import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

from rich.console import Console
from rich.table import Table

from email_sort import __version__
from email_sort.config import get_config, get_config_dir, get_config_path
from email_sort.db import EMAIL_TABLE, _get_db_path, get_db
from email_sort.log import setup_logging

console = Console()


def command_ingest(args):
    if args.source == "fastmail":
        from email_sort.ingest_fastmail import ingest_fastmail

        ingest_fastmail(args.name)
    elif args.source == "mbox":
        from email_sort.ingest_mbox import parse_mbox

        parse_mbox(args.mbox_path, args.source_name)
    elif args.source == "imap":
        from email_sort.ingest_imap import ingest_imap

        ingest_imap(watch=args.watch, source=args.name)


def command_classify(args):
    from email_sort.classify import classify_emails

    classify_emails(args.limit, args.source, args.window, args.reclassify)


def _parse_int_list(value: str) -> list[int]:
    try:
        items = [int(item.strip()) for item in value.split(",") if item.strip()]
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected comma-separated integers") from exc
    if not items:
        raise argparse.ArgumentTypeError("expected at least one integer")
    if any(item <= 0 for item in items):
        raise argparse.ArgumentTypeError("all integers must be positive")
    return items


def _parse_str_list(value: str) -> list[str]:
    items = [item.strip() for item in value.split(",") if item.strip()]
    if not items:
        raise argparse.ArgumentTypeError("expected at least one value")
    return items


def command_benchmark_classification(args):
    from email_sort.benchmark import benchmark_classification

    result = benchmark_classification(
        server_name=args.server,
        caps=args.caps,
        sample_count=args.samples,
        models=args.models,
        output_dir=args.output_dir,
        source=args.source,
        timeout=args.timeout,
        max_tokens=args.max_tokens,
    )
    console.print(f"Benchmark CSV: [cyan]{result['csv_path']}[/cyan]")
    console.print(f"Benchmark Markdown: [cyan]{result['markdown_path']}[/cyan]")
    console.print(f"Rows: [green]{result['rows']}[/green]")


def command_detect_language(args):
    from email_sort.detect_language import detect_languages

    detect_languages(args.source, args.batch)


def command_init_db(args):
    from email_sort.db import init_db

    init_db()
    console.print(f"Database initialized at [cyan]{_get_db_path()}[/cyan]")


def command_migrate(args):
    from email_sort.migrate import migrate

    migrate()


def command_heuristics(args):
    from email_sort.heuristics import run_heuristics

    run_heuristics(recompute=args.recompute)


def command_analyze_senders(args):
    from email_sort.sender_analysis import analyze_all_senders

    stats = analyze_all_senders()
    console.print(
        f"Analyzed [green]{stats['total_senders']}[/green] senders and [green]{stats['total_domains']}[/green] domains"
    )
    table = Table(title="Top Spam/Promotional Senders")
    table.add_column("Sender")
    table.add_column("Emails", justify="right")
    table.add_column("Spam Ratio", justify="right")
    for row in stats["top_spam"]:
        table.add_row(row["sender"], str(row["total_emails"]), f"{row['spam_ratio']:.1%}")
    console.print(table)


def command_correct(args):
    from email_sort.corrections import create_correction

    result = create_correction(args.message_id, args.category, args.action)
    console.print(
        f"Corrected [cyan]{args.message_id}[/cyan]: {result['original_category']}/{result['original_action']} -> "
        f"[green]{result['corrected_category']}[/green]/[green]{result['corrected_action']}[/green]"
    )
    if result["overrides"]:
        console.print(f"Created/updated overrides: {', '.join(result['overrides'])}")


def command_corrections_list(args):
    from email_sort.corrections import list_corrections

    table = Table(title="Corrections")
    for column in ("Message ID", "Original", "Corrected", "Corrected At"):
        table.add_column(column)
    for row in list_corrections():
        table.add_row(
            row["message_id"],
            f"{row['original_category']}/{row['original_action']}",
            f"{row['corrected_category']}/{row['corrected_action']}",
            row["corrected_at"],
        )
    console.print(table)


def command_corrections_export(args):
    from email_sort.corrections import export_corrections_jsonl

    output = Path(args.output or "out/corrections.jsonl")
    output.parent.mkdir(parents=True, exist_ok=True)
    data = export_corrections_jsonl()
    output.write_text(data + ("\n" if data else ""))
    console.print(f"Exported corrections to [cyan]{output}[/cyan]")


def command_unsubscribe(args):
    from email_sort.unsubscribe_agent import process_unsubscribe_list

    result = asyncio.run(
        process_unsubscribe_list(dry_run=not args.execute, execute=args.execute, yes=args.yes)
    )
    if result.get("dry_run"):
        console.print(
            f"Dry run found [green]{result['total_found']}[/green] unsubscribe candidates"
        )
        table = Table(title="Sample Candidates")
        table.add_column("Sender")
        table.add_column("Category")
        table.add_column("List-Unsubscribe")
        for row in result.get("sample", []):
            table.add_row(
                row.get("sender") or "",
                row.get("category") or "",
                row.get("list_unsubscribe") or "",
            )
        console.print(table)
    elif result.get("cancelled"):
        console.print("Unsubscribe execution cancelled")
    else:
        console.print(
            f"Attempted [cyan]{result['attempted']}[/cyan]; "
            f"successful [green]{len(result['successful'])}[/green]; failed [red]{len(result['failed'])}[/red]"
        )


def command_verify_unsubscribes(args):
    from email_sort.verify_unsubscribe import check_failed_unsubscribes

    failed = check_failed_unsubscribes()
    if not failed:
        console.print("[green]No failed unsubscribes found.[/green]")
        return
    table = Table(title="Failed Unsubscribes")
    table.add_column("Sender")
    table.add_column("Unsubscribed At")
    table.add_column("Emails After", justify="right")
    table.add_column("Last Received")
    for row in failed:
        table.add_row(
            row["sender"],
            row["unsubscribed_at"],
            str(row["email_count"]),
            row["last_received"] or "",
        )
    console.print(table)
    console.print(
        "Suggested escalation: add these senders to server-side block list or a Sieve reject rule."
    )


def command_sieve(args):
    from email_sort.sieve_generator import diff_sieve, generate_sieve, upload_sieve

    if args.sieve_action == "generate":
        generate_sieve(args.output)
        console.print(f"Generated Sieve script at [cyan]{args.output}[/cyan]")
    elif args.sieve_action == "upload":
        upload_sieve(args.output)
        console.print(f"Uploaded [cyan]{args.output}[/cyan] as active ManageSieve script")
    elif args.sieve_action == "diff":
        diff = diff_sieve(args.output)
        console.print(diff or "No differences")


def command_export(args):
    from email_sort.export import export_ban_list, export_results, export_unsubscribe_list

    if args.export_type in {None, "all"}:
        export_results()
    elif args.export_type == "ban-list":
        export_ban_list()
    elif args.export_type == "unsubscribe-list":
        export_unsubscribe_list()
    elif args.export_type == "corrections":
        command_corrections_export(args)
    else:
        export_results()


def command_stats(args):
    conn = get_db()
    try:
        cursor = conn.cursor()
        table = Table(title="Email Stats by Source")
        table.add_column("Source")
        table.add_column("Total", justify="right")
        table.add_column("LLM Classified", justify="right")
        table.add_column("Rule", justify="right")
        table.add_column("Heuristic", justify="right")
        table.add_column("Duplicates", justify="right")
        table.add_column("Digests", justify="right")
        cursor.execute(
            f"""
            SELECT source,
                   COUNT(*) AS total,
                   SUM(CASE WHEN classify_model IS NOT NULL AND classify_model != '' THEN 1 ELSE 0 END) AS classified,
                   SUM(CASE WHEN rule_category IS NOT NULL AND rule_category != '' THEN 1 ELSE 0 END) AS rule_classified,
                   SUM(CASE WHEN heuristic_category IS NOT NULL AND heuristic_category != '' THEN 1 ELSE 0 END) AS heuristic,
                   SUM(CASE WHEN is_duplicate = 1 THEN 1 ELSE 0 END) AS duplicates,
                   SUM(CASE WHEN is_digest = 1 THEN 1 ELSE 0 END) AS digests
            FROM {EMAIL_TABLE}
            GROUP BY source
            ORDER BY total DESC
            """
        )
        for row in cursor.fetchall():
            table.add_row(
                row["source"],
                str(row["total"]),
                str(row["classified"] or 0),
                str(row["rule_classified"] or 0),
                str(row["heuristic"] or 0),
                str(row["duplicates"] or 0),
                str(row["digests"] or 0),
            )
        console.print(table)

        category_table = Table(title="Category Distribution by Source")
        category_table.add_column("Classification Source")
        category_table.add_column("Category")
        category_table.add_column("Count", justify="right")
        cursor.execute(
            """
            SELECT classification_source, category, COUNT(*) AS count FROM (
                SELECT 'LLM' AS classification_source, category FROM emails
                WHERE classify_model IS NOT NULL AND classify_model != ''
                UNION ALL
                SELECT 'Rule' AS classification_source, rule_category AS category FROM emails
                WHERE rule_category IS NOT NULL AND rule_category != ''
                UNION ALL
                SELECT 'Heuristic' AS classification_source, heuristic_category AS category FROM emails
                WHERE heuristic_category IS NOT NULL AND heuristic_category != ''
            )
            WHERE category IS NOT NULL AND category != ''
            GROUP BY classification_source, category
            ORDER BY classification_source, count DESC
            """
        )
        for row in cursor.fetchall():
            category_table.add_row(row["classification_source"], row["category"], str(row["count"]))
        console.print(category_table)
    finally:
        conn.close()


def command_config(args):
    config = get_config()
    table = Table(title="Email Sort Configuration")
    table.add_column("Path")
    table.add_column("Value")
    table.add_row("Config File", str(get_config_path()))
    table.add_row("Config Dir", str(get_config_dir()))
    table.add_row("Database", str(_get_db_path()))
    table.add_row("Classification Log", str(get_config_dir() / "classification.log"))
    table.add_row("Log Level", config.general.log_level)
    table.add_row("Servers", ", ".join(server.name for server in config.servers) or "<none>")
    console.print(table)


def command_precheck(args):
    from email_sort.precheck import run_precheck

    ok, results = run_precheck(check_servers=args.check_servers)
    for result in results:
        console.print(result)
    if not ok:
        raise SystemExit(1)


def command_watch(args):
    while True:
        console.clear()
        command_stats(args)
        time.sleep(5)


def build_parser():
    parser = argparse.ArgumentParser(description="email-sort CLI toolkit")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug-level logging for this run",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Suppress console output and only log errors",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Override the configured log level",
    )
    subparsers = parser.add_subparsers(dest="command")

    ingest = subparsers.add_parser(
        "ingest",
        help="Ingest emails",
        description="Import email metadata and bodies into the local SQLite database.",
    )
    ingest_sub = ingest.add_subparsers(dest="source", required=True)
    fastmail = ingest_sub.add_parser(
        "fastmail",
        help="Ingest from Fastmail JMAP",
        description="Fetch messages from Fastmail using the configured JMAP API token.",
    )
    fastmail.add_argument(
        "--name",
        "--source",
        default="fastmail",
        help="Source name to store for imported Fastmail messages (default: fastmail)",
    )
    fastmail.set_defaults(func=command_ingest)
    mbox = ingest_sub.add_parser(
        "mbox",
        help="Ingest from an mbox file",
        description="Parse a local mbox export such as Google Takeout and import its messages.",
    )
    mbox.add_argument("mbox_path", help="Path to the mbox file to import")
    mbox.add_argument(
        "--source",
        dest="source_name",
        default="gmail",
        help="Source name to store for imported mbox messages (default: gmail)",
    )
    mbox.set_defaults(func=command_ingest)
    imap = ingest_sub.add_parser(
        "imap",
        help="Ingest from IMAP",
        description="Import messages from configured IMAP folders.",
    )
    imap.add_argument("--watch", action="store_true", help="Poll configured folders continuously")
    imap.add_argument(
        "--name",
        "--source",
        default="imap",
        help="Source name to store for imported IMAP messages (default: imap)",
    )
    imap.set_defaults(func=command_ingest)

    classify = subparsers.add_parser(
        "classify",
        help="Classify emails",
        description="Classify unprocessed emails using configured OpenAI-compatible LLM servers.",
    )
    classify.add_argument("--limit", type=int, help="Maximum number of emails to classify")
    classify.add_argument("--source", help="Only classify emails from this source")
    classify.add_argument(
        "--window",
        type=int,
        default=100,
        help="Number of status lines to keep in the live display (default: 100)",
    )
    classify.add_argument(
        "--reclassify",
        help="Clear existing classifications first; use 'all' or comma-separated categories",
    )
    classify.set_defaults(func=command_classify)

    benchmark = subparsers.add_parser(
        "benchmark-classification",
        help="Benchmark LLM classification output",
        description="Benchmark configured OpenAI-compatible servers using the full classification prompt and real email body samples.",
    )
    benchmark.add_argument("server", help="Configured server name to benchmark, such as m5")
    benchmark.add_argument(
        "--caps",
        type=_parse_int_list,
        default=[500, 1000, 2000, 4000, 8000, 12000],
        help="Comma-separated body character caps (default: 500,1000,2000,4000,8000,12000)",
    )
    benchmark.add_argument(
        "--samples",
        type=int,
        default=3,
        help="Number of real emails to benchmark (default: 3)",
    )
    benchmark.add_argument(
        "--models",
        type=_parse_str_list,
        help="Comma-separated model IDs to benchmark (default: all non-embedding models on server)",
    )
    benchmark.add_argument(
        "--source",
        help="Only sample emails from this source",
    )
    benchmark.add_argument(
        "--output-dir",
        default="out",
        help="Directory for CSV and Markdown artifacts (default: out)",
    )
    benchmark.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help="Per-request timeout in seconds (default: 300)",
    )
    benchmark.add_argument(
        "--max-tokens",
        type=int,
        default=256,
        help="Maximum output tokens per request (default: 256)",
    )
    benchmark.set_defaults(func=command_benchmark_classification)

    lang = subparsers.add_parser(
        "detect-language",
        help="Detect email languages",
        description="Populate missing email language values with the fastText language model.",
    )
    lang.add_argument("--source", help="Only process emails from this source")
    lang.add_argument(
        "--batch",
        type=int,
        default=1000,
        help="Number of emails to process per database batch (default: 1000)",
    )
    lang.set_defaults(func=command_detect_language)

    subparsers.add_parser(
        "init-db",
        help="Initialize the database",
        description="Create the local SQLite database schema if it does not already exist.",
    ).set_defaults(func=command_init_db)
    subparsers.add_parser(
        "migrate",
        help="Run database migrations",
        description="Apply schema migrations to the configured local SQLite database.",
    ).set_defaults(func=command_migrate)
    heuristics = subparsers.add_parser(
        "heuristics",
        help="Run heuristic classifiers",
        description="Apply fast local rules for language, automation, duplicates, digests, and spam signals.",
    )
    heuristics.add_argument(
        "--recompute",
        action="store_true",
        help="Re-run the expensive per-email heuristic pass for all emails",
    )
    heuristics.set_defaults(func=command_heuristics)
    subparsers.add_parser(
        "analyze-senders",
        help="Analyze sender reputation",
        description="Aggregate sender and domain statistics used by exports and Sieve generation.",
    ).set_defaults(func=command_analyze_senders)

    correct = subparsers.add_parser(
        "correct",
        help="Correct one classification",
        description="Record a manual correction for one email and update sender overrides when repeated.",
    )
    correct.add_argument("message_id", help="Message ID or provider ID of the email to correct")
    correct.add_argument("--category", required=True, help="Correct category to store")
    correct.add_argument("--action", required=True, help="Correct action to store")
    correct.set_defaults(func=command_correct)

    corrections = subparsers.add_parser(
        "corrections",
        help="Manage manual corrections",
        description="List or export classification corrections for review and fine-tuning data.",
    )
    corr_sub = corrections.add_subparsers(dest="corrections_action", required=True)
    corr_sub.add_parser(
        "list",
        help="List recorded corrections",
        description="Print all manual corrections ordered by most recent first.",
    ).set_defaults(func=command_corrections_list)
    corr_export = corr_sub.add_parser(
        "export",
        help="Export corrections as JSONL",
        description="Write manual corrections to a JSONL file for review or model training.",
    )
    corr_export.add_argument(
        "--output",
        "-o",
        help="Output JSONL path (default: out/corrections.jsonl)",
    )
    corr_export.set_defaults(func=command_corrections_export)

    unsub = subparsers.add_parser(
        "unsubscribe",
        help="Process unsubscribe candidates",
        description="Find and optionally execute safe unsubscribe actions for promotional senders.",
    )
    mode = unsub.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run", action="store_true", help="Preview candidates without taking action"
    )
    mode.add_argument("--execute", action="store_true", help="Attempt unsubscribe actions")
    unsub.add_argument("--yes", action="store_true", help="Skip interactive confirmation prompts")
    unsub.set_defaults(func=command_unsubscribe)

    subparsers.add_parser(
        "verify-unsubscribes",
        help="Check unsubscribe failures",
        description="Find senders that continued sending mail after a successful unsubscribe attempt.",
    ).set_defaults(func=command_verify_unsubscribes)

    sieve = subparsers.add_parser(
        "sieve",
        help="Generate or upload Sieve filters",
        description="Create, diff, or upload ManageSieve filters from sender analysis and overrides.",
    )
    sieve_sub = sieve.add_subparsers(dest="sieve_action", required=True)
    sieve_help = {
        "generate": "Generate a local Sieve script",
        "upload": "Upload a Sieve script to the server",
        "diff": "Compare local script with active remote script",
    }
    for action, help_text in sieve_help.items():
        child = sieve_sub.add_parser(action, help=help_text, description=help_text + ".")
        child.add_argument(
            "--output",
            "-o",
            default="out/filters.sieve",
            help="Path to the local Sieve script (default: out/filters.sieve)",
        )
        child.set_defaults(func=command_sieve)

    export = subparsers.add_parser(
        "export",
        help="Export reports",
        description="Write CSV or JSONL report files from the classified email database.",
    )
    export.set_defaults(func=command_export, export_type=None)
    export_sub = export.add_subparsers(dest="export_type")
    export_sub.add_parser(
        "all",
        help="Export all reports",
        description="Export sender reputation, ban list, and unsubscribe list reports.",
    ).set_defaults(func=command_export)
    export_sub.add_parser(
        "ban-list",
        help="Export sender/domain ban candidates",
        description="Write domains and senders that look unsafe or unwanted to CSV.",
    ).set_defaults(func=command_export)
    export_sub.add_parser(
        "unsubscribe-list",
        help="Export unsubscribe candidates",
        description="Write senders with unsubscribe links and promotional classifications to CSV.",
    ).set_defaults(func=command_export)
    export_corr = export_sub.add_parser(
        "corrections",
        help="Export corrections as JSONL",
        description="Write manual corrections to a JSONL file.",
    )
    export_corr.add_argument(
        "--output",
        "-o",
        help="Output JSONL path (default: out/corrections.jsonl)",
    )
    export_corr.set_defaults(func=command_export)

    subparsers.add_parser(
        "stats",
        help="Show database statistics",
        description="Print source and category totals from the local email database.",
    ).set_defaults(func=command_stats)
    subparsers.add_parser(
        "config",
        help="Show resolved configuration",
        description="Print config paths, database path, log paths, and configured LLM servers.",
    ).set_defaults(func=command_config)
    precheck = subparsers.add_parser(
        "precheck",
        help="Check local setup",
        description="Validate configuration, writable paths, language model availability, and optional servers.",
    )
    precheck.add_argument(
        "--check-servers",
        action="store_true",
        help="Perform HTTP health checks against configured LLM servers",
    )
    precheck.set_defaults(func=command_precheck)
    subparsers.add_parser(
        "watch",
        help="Watch live statistics",
        description="Refresh database statistics every five seconds until interrupted.",
    ).set_defaults(func=command_watch)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    config = get_config()
    log_level = args.log_level or ("DEBUG" if args.verbose else config.general.log_level)
    if args.quiet:
        sys.stdout = open(os.devnull, "w")
        console.file = sys.stdout
        log_level = "ERROR"
    setup_logging(level=log_level, log_file=str(get_config_dir() / "email-sort.log"))
    if not hasattr(args, "func"):
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
