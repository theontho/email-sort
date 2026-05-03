import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

from rich.console import Console
from rich.table import Table

from email_sort.config import get_config_dir, get_config_path
from email_sort.db import DB_PATH, EMAIL_TABLE, get_db


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


def command_detect_language(args):
    from email_sort.detect_language import detect_languages

    detect_languages(args.source, args.batch)


def command_init_db(args):
    from email_sort.db import init_db

    init_db()
    console.print(f"Database initialized at [cyan]{DB_PATH}[/cyan]")


def command_migrate(args):
    from email_sort.migrate import migrate

    migrate()


def command_heuristics(args):
    from email_sort.heuristics import run_heuristics

    run_heuristics()


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
        table.add_column("Heuristic", justify="right")
        table.add_column("Duplicates", justify="right")
        table.add_column("Digests", justify="right")
        cursor.execute(
            f"""
            SELECT source,
                   COUNT(*) AS total,
                   SUM(CASE WHEN category IS NOT NULL AND category != '' THEN 1 ELSE 0 END) AS classified,
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
                str(row["heuristic"] or 0),
                str(row["duplicates"] or 0),
                str(row["digests"] or 0),
            )
        console.print(table)

        category_table = Table(title="Category Distribution")
        category_table.add_column("Category")
        category_table.add_column("Count", justify="right")
        cursor.execute(
            """
            SELECT category, COUNT(*) AS count FROM (
                SELECT category FROM emails
            ) WHERE category IS NOT NULL AND category != ''
            GROUP BY category ORDER BY count DESC
            """
        )
        for row in cursor.fetchall():
            category_table.add_row(row["category"], str(row["count"]))
        console.print(category_table)
    finally:
        conn.close()


def command_config(args):
    table = Table(title="Email Sort Configuration")
    table.add_column("Path")
    table.add_column("Value")
    table.add_row("Config File", str(get_config_path()))
    table.add_row("Config Dir", str(get_config_dir()))
    table.add_row("Database", str(DB_PATH))
    table.add_row("Classification Log", str(get_config_dir() / "classification.log"))
    console.print(table)


def command_watch(args):
    while True:
        os.system("cls" if os.name == "nt" else "clear")
        command_stats(args)
        time.sleep(5)


def build_parser():
    parser = argparse.ArgumentParser(description="email-sort CLI toolkit")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--quiet", "-q", action="store_true")
    subparsers = parser.add_subparsers(dest="command")

    ingest = subparsers.add_parser("ingest", help="Ingest emails")
    ingest_sub = ingest.add_subparsers(dest="source", required=True)
    fastmail = ingest_sub.add_parser("fastmail", help="Ingest from Fastmail JMAP")
    fastmail.add_argument("--name", "--source", default="fastmail")
    fastmail.set_defaults(func=command_ingest)
    mbox = ingest_sub.add_parser("mbox", help="Ingest from mbox")
    mbox.add_argument("mbox_path")
    mbox.add_argument("--source", dest="source_name", default="gmail")
    mbox.add_argument("--table", dest="source_name", help=argparse.SUPPRESS)
    mbox.set_defaults(func=command_ingest)
    imap = ingest_sub.add_parser("imap", help="Ingest from IMAP")
    imap.add_argument("--watch", action="store_true")
    imap.add_argument("--name", "--source", default="imap")
    imap.add_argument("--table", dest="name", help=argparse.SUPPRESS)
    imap.set_defaults(func=command_ingest)

    classify = subparsers.add_parser("classify", help="Classify emails")
    classify.add_argument("--limit", type=int)
    classify.add_argument("--source")
    classify.add_argument("--table", dest="source", help=argparse.SUPPRESS)
    classify.add_argument("--window", type=int, default=100)
    classify.add_argument("--reclassify")
    classify.set_defaults(func=command_classify)

    lang = subparsers.add_parser("detect-language")
    lang.add_argument("--source")
    lang.add_argument("--table", dest="source", help=argparse.SUPPRESS)
    lang.add_argument("--batch", type=int, default=1000)
    lang.set_defaults(func=command_detect_language)

    subparsers.add_parser("init-db").set_defaults(func=command_init_db)
    subparsers.add_parser("migrate").set_defaults(func=command_migrate)
    subparsers.add_parser("heuristics").set_defaults(func=command_heuristics)
    subparsers.add_parser("analyze-senders").set_defaults(func=command_analyze_senders)

    correct = subparsers.add_parser("correct")
    correct.add_argument("message_id")
    correct.add_argument("--category", required=True)
    correct.add_argument("--action", required=True)
    correct.set_defaults(func=command_correct)

    corrections = subparsers.add_parser("corrections")
    corr_sub = corrections.add_subparsers(dest="corrections_action", required=True)
    corr_sub.add_parser("list").set_defaults(func=command_corrections_list)
    corr_export = corr_sub.add_parser("export")
    corr_export.add_argument("--output", "-o")
    corr_export.set_defaults(func=command_corrections_export)

    unsub = subparsers.add_parser("unsubscribe")
    mode = unsub.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    unsub.add_argument("--yes", action="store_true")
    unsub.set_defaults(func=command_unsubscribe)

    subparsers.add_parser("verify-unsubscribes").set_defaults(func=command_verify_unsubscribes)

    sieve = subparsers.add_parser("sieve")
    sieve_sub = sieve.add_subparsers(dest="sieve_action", required=True)
    for action in ("generate", "upload", "diff"):
        child = sieve_sub.add_parser(action)
        child.add_argument("--output", "-o", default="out/filters.sieve")
        child.set_defaults(func=command_sieve)

    export = subparsers.add_parser("export")
    export.set_defaults(func=command_export, export_type=None)
    export_sub = export.add_subparsers(dest="export_type")
    export_sub.add_parser("all").set_defaults(func=command_export)
    export_sub.add_parser("ban-list").set_defaults(func=command_export)
    export_sub.add_parser("unsubscribe-list").set_defaults(func=command_export)
    export_corr = export_sub.add_parser("corrections")
    export_corr.add_argument("--output", "-o")
    export_corr.set_defaults(func=command_export)

    subparsers.add_parser("stats").set_defaults(func=command_stats)
    subparsers.add_parser("config").set_defaults(func=command_config)
    subparsers.add_parser("watch").set_defaults(func=command_watch)

    legacy_fastmail = subparsers.add_parser("ingest-fastmail")
    legacy_fastmail.add_argument("--source", default="fastmail")
    legacy_fastmail.set_defaults(
        func=lambda args: command_ingest(argparse.Namespace(source="fastmail", name=args.source))
    )
    legacy_mbox = subparsers.add_parser("ingest-mbox")
    legacy_mbox.add_argument("mbox_path")
    legacy_mbox.add_argument("--source", default="gmail")
    legacy_mbox.add_argument("--table", dest="source", help=argparse.SUPPRESS)
    legacy_mbox.set_defaults(
        func=lambda args: command_ingest(
            argparse.Namespace(source="mbox", mbox_path=args.mbox_path, source_name=args.source)
        )
    )

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    if args.quiet:
        sys.stdout = open(os.devnull, "w")
        console.file = sys.stdout
    if not hasattr(args, "func"):
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
