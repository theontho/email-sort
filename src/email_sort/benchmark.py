import csv
import json
import re
import shutil
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from openai import OpenAI

from email_sort.classify import parse_classification
from email_sort.config import get_servers, get_setting
from email_sort.db import EMAIL_TABLE, get_db
from email_sort.progress import make_progress

CLASSIFICATION_SYSTEM_PROMPT = (
    "You are a highly efficient email classifier. Output ONLY five items, comma separated: "
    "category, confidence, suggested_category, summary, action.\n"
    "Categories: Finance, Health, Work, Newsletter, Promotional, Social, Home, Education, "
    "Tech, Shopping, Travel, Security, Shipping, Personal, Spam, Other.\n"
    "Action (Message Type): Authentication, Mandatory, Informational, Newsletter, Promotional, "
    "Social, Personal.\n"
    "Example: 'Security, 0.98, Password Reset, Account password reset link, Authentication'.\n"
    "DO NOT include any other text. suggested_category should be 2-4 words. summary should be "
    "a short plain-language email summary. Do not use commas inside any field."
)

CSV_FIELDNAMES = [
    "run_id",
    "server_name",
    "server_url",
    "model",
    "loaded_context_length",
    "max_context_length",
    "email_id",
    "email_date",
    "subject_chars",
    "snippet_chars",
    "body_chars",
    "body_cap_chars",
    "prompt_chars",
    "elapsed_seconds",
    "raw_output_chars",
    "status",
    "error",
    "raw_output",
    "parsed_category",
    "parsed_confidence",
    "parsed_suggested_category",
    "parsed_summary",
    "parsed_action",
    "parse_valid",
]

BACKENDS = ("openai", "opencode")
BENCHMARK_EMAIL_EXCERPT_CHARS = 2000


def _opencode_executable() -> str:
    executable = shutil.which("opencode")
    if not executable:
        raise RuntimeError("opencode executable not found on PATH")
    return executable


def _markdown_escape(value: Any) -> str:
    return str(value or "").replace("|", "\\|").replace("\n", "<br>")


def _email_excerpt(
    value: str, limit: int = BENCHMARK_EMAIL_EXCERPT_CHARS, redact: bool = False
) -> str:
    text = value or ""
    if redact:
        text = re.sub(r"[\w.+-]+@[\w.-]+", "[email]", text)
        text = re.sub(r"https?://\S+", "[url]", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > limit:
        return text[: limit - 1].rstrip() + "…"
    return text


def _server_by_name(server_name: str) -> dict[str, Any]:
    for server in get_servers():
        if server.get("name") == server_name and not server.get("disabled", False):
            return server
    enabled = [server.get("name") for server in get_servers() if not server.get("disabled", False)]
    available = ", ".join(str(name) for name in enabled) or "none"
    raise ValueError(
        f"No enabled server named {server_name!r} found. Available servers: {available}"
    )


def _model_context(base_url: str, model_id: str) -> dict[str, Any]:
    try:
        response = requests.get(f"{base_url}/api/v0/models", timeout=10)
        response.raise_for_status()
        for item in response.json().get("data", []):
            if item.get("id") == model_id:
                return {
                    "state": item.get("state"),
                    "loaded_context_length": item.get("loaded_context_length"),
                    "max_context_length": item.get("max_context_length"),
                }
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}
    return {}


def _sample_rows(sample_count: int, min_body_chars: int, source: str | None) -> list[dict]:
    source_filter = "AND source = ?" if source else ""
    params: list[Any] = [min_body_chars]
    if source:
        params.append(source)
    params.append(sample_count)
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT id, sender, subject, date, snippet, body_text
            FROM {EMAIL_TABLE}
            WHERE body_text IS NOT NULL
              AND body_text != ''
              AND LENGTH(body_text) > LENGTH(COALESCE(snippet, ''))
              AND LENGTH(body_text) >= ?
              AND language = 'en'
              {source_filter}
            ORDER BY id DESC
            LIMIT ?
            """,
            params,
        )
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def _available_chat_models(client: OpenAI) -> list[str]:
    return [
        model.id
        for model in client.models.list().data
        if "embedding" not in model.id.lower() and "embed" not in model.id.lower()
    ]


def available_opencode_models(provider: str | None = None) -> list[str]:
    command = [_opencode_executable(), "models"]
    if provider:
        command.append(provider)
    result = subprocess.run(  # noqa: S603 - argv is explicit; optional provider is not a shell string.
        command, check=True, capture_output=True, text=True, timeout=60
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _opencode_classify(model: str, prompt: str, timeout: float, agent: str) -> str:
    message = f"{CLASSIFICATION_SYSTEM_PROMPT}\n\n{prompt}"
    result = subprocess.run(  # noqa: S603 - argv is explicit; prompt/model are not shell strings.
        [
            _opencode_executable(),
            "run",
            "--pure",
            "--model",
            model,
            "--agent",
            agent,
            "--format",
            "json",
            message,
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    texts = []
    for line in result.stdout.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") == "text":
            text = event.get("part", {}).get("text")
            if text:
                texts.append(text)
    return "\n".join(texts).strip()


def _write_markdown(
    csv_path: Path,
    md_path: Path,
    metadata: dict[str, Any],
    models: list[str],
    caps: list[int],
    model_contexts: dict[str, dict[str, Any]],
) -> None:
    if not csv_path.exists():
        return
    with csv_path.open(newline="", encoding="utf-8") as file:
        records = list(csv.DictReader(file))

    ok_records = [row for row in records if row["status"] == "OK"]
    by_model_cap: dict[tuple[str, int], list[float]] = defaultdict(list)
    parse_counts: dict[tuple[str, int], dict[str, int]] = defaultdict(
        lambda: {"valid": 0, "total": 0}
    )
    for row in ok_records:
        key = (row["model"], int(row["body_cap_chars"]))
        by_model_cap[key].append(float(row["elapsed_seconds"]))
        parse_counts[key]["total"] += 1
        if row["parse_valid"] == "true":
            parse_counts[key]["valid"] += 1

    lines = [
        "# Classification Benchmark",
        "",
        f"- Run ID: `{metadata['run_id']}`",
        f"- Backend: `{metadata['backend']}`",
        f"- Server: `{metadata['server_name']}` `{metadata['server_url']}`",
        f"- Samples: `{metadata['sample_count']}` real emails where `body_text` is longer than `snippet`",
        f"- Caps: `{', '.join(str(cap) for cap in caps)}` body characters",
        f"- Max output tokens: `{metadata['max_tokens']}`",
        f"- CSV: `{csv_path}`",
        "",
        "## Model Contexts",
        "",
        "| Model | State | Loaded Context | Max Context |",
        "|---|---|---:|---:|",
    ]
    for model in models:
        context = model_contexts.get(model, {})
        lines.append(
            f"| `{model}` | {context.get('state', '')} | "
            f"{context.get('loaded_context_length', '')} | {context.get('max_context_length', '')} |"
        )

    lines.extend(
        [
            "",
            "## Samples",
            "",
            "| Email ID | Date | Subject Chars | Snippet Chars | Body Chars |",
            "|---:|---|---:|---:|---:|",
        ]
    )
    for sample in metadata["samples"]:
        lines.append(
            f"| {sample['email_id']} | {sample['email_date']} | "
            f"{sample['subject_chars']} | {sample['snippet_chars']} | {sample['body_chars']} |"
        )

    lines.extend(["", "## Benchmark Email Inputs"])
    for sample in metadata["samples"]:
        lines.extend(
            [
                "",
                f"### Email {sample['email_id']}",
                "",
                f"- Date: `{sample['email_date']}`",
                f"- Sender: `{_markdown_escape(sample.get('sender', ''))}`",
                f"- Subject: {_markdown_escape(sample.get('subject', ''))}",
                f"- Body chars: `{sample['body_chars']}`",
                "",
                "```text",
                sample.get("body_excerpt", ""),
                "```",
            ]
        )

    lines.extend(
        [
            "",
            "## Timing And Parse Summary",
            "",
            "| Model | Cap Chars | Count | Parsed | Median s | Mean s | Min s | Max s |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for model in models:
        for cap in caps:
            times = by_model_cap.get((model, cap), [])
            parsed = parse_counts.get((model, cap), {"valid": 0, "total": 0})
            if times:
                lines.append(
                    f"| `{model}` | {cap} | {len(times)} | "
                    f"{parsed['valid']}/{parsed['total']} | {statistics.median(times):.3f} | "
                    f"{statistics.mean(times):.3f} | {min(times):.3f} | {max(times):.3f} |"
                )
            else:
                lines.append(f"| `{model}` | {cap} | 0 | 0/0 |  |  |  |  |")

    errors = [row for row in records if row["status"] != "OK"]
    if errors:
        lines.extend(
            [
                "",
                "## Errors",
                "",
                "| Model | Cap Chars | Email ID | Status | Error |",
                "|---|---:|---:|---|---|",
            ]
        )
        for row in errors:
            lines.append(
                f"| `{row['model']}` | {row['body_cap_chars']} | {row['email_id']} | "
                f"{row['status']} | {_markdown_escape(row['error'])} |"
            )

    lines.extend(
        [
            "",
            "## Parsed Outputs",
            "",
            "| Model | Email ID | Cap | Seconds | Category | Confidence | Title | Action | Summary |",
            "|---|---:|---:|---:|---|---:|---|---|---|",
        ]
    )
    for row in records:
        lines.append(
            f"| `{row['model']}` | {row['email_id']} | {row['body_cap_chars']} | "
            f"{row['elapsed_seconds']} | {_markdown_escape(row['parsed_category'])} | "
            f"{row['parsed_confidence']} | {_markdown_escape(row['parsed_suggested_category'])} | "
            f"{_markdown_escape(row['parsed_action'])} | {_markdown_escape(row['parsed_summary'])} |"
        )

    lines.extend(
        [
            "",
            "## Raw Outputs",
            "",
            "| Model | Email ID | Cap | Raw Output |",
            "|---|---:|---:|---|",
        ]
    )
    for row in records:
        lines.append(
            f"| `{row['model']}` | {row['email_id']} | {row['body_cap_chars']} | "
            f"{_markdown_escape(row['raw_output'])} |"
        )

    metadata = dict(metadata)
    metadata["model_contexts"] = model_contexts
    lines.extend(
        ["", "## Metadata", "", "```json", json.dumps(metadata, indent=2, sort_keys=True), "```"]
    )
    md_path.write_text("\n".join(lines), encoding="utf-8")


def benchmark_classification(
    server_name: str | None,
    caps: list[int],
    sample_count: int = 3,
    models: list[str] | None = None,
    output_dir: str | Path = "out",
    source: str | None = None,
    timeout: float = 300.0,
    max_tokens: int = 256,
    backend: str = "openai",
    opencode_agent: str = "summary",
    opencode_provider: str | None = None,
    progress: bool = True,
    redact_inputs: bool = False,
) -> dict[str, Path | int]:
    if not caps:
        raise ValueError("At least one body cap is required")
    if backend not in BACKENDS:
        raise ValueError(f"Unsupported benchmark backend {backend!r}")
    if not server_name:
        if backend == "openai":
            servers = [server for server in get_servers() if not server.get("disabled", False)]
            if len(servers) != 1:
                raise ValueError(
                    "server is required when zero or multiple OpenAI servers are configured"
                )
            server_name = servers[0]["name"]
        else:
            server_name = opencode_provider or "opencode"
    server = (
        _server_by_name(server_name)
        if backend == "openai"
        else {"name": server_name, "url": f"opencode:{server_name}"}
    )
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_path / f"{server_name}_classification_benchmark_{run_id}.csv"
    md_path = output_path / f"{server_name}_classification_benchmark_{run_id}.md"

    client = None
    if backend == "openai":
        client = OpenAI(
            base_url=server["url"],
            api_key=server.get("api_key") or get_setting("lmstudio_key", "lm-studio"),
        )
        benchmark_models = models or _available_chat_models(client)
    else:
        benchmark_models = models or available_opencode_models(opencode_provider)
    rows = _sample_rows(sample_count, max(caps), source)
    if len(rows) < sample_count:
        raise ValueError(f"Only found {len(rows)} benchmark samples")

    base_url = server["url"].rsplit("/v1", 1)[0]
    model_contexts: dict[str, dict[str, Any]] = {}
    total_requests = len(rows) * len(caps) * len(benchmark_models)
    completed_requests = 0
    started_at = time.time()
    last_log_at = started_at
    progress_bar = None
    progress_task = None
    use_progress_bar = progress and sys.stdout.isatty()
    if use_progress_bar:
        progress_bar = make_progress(spinner=True)
        progress_bar.start()
        progress_task = progress_bar.add_task("Benchmarking classification", total=total_requests)
    metadata = {
        "run_id": run_id,
        "backend": backend,
        "server_name": server_name,
        "server_url": server["url"],
        "models": benchmark_models,
        "caps": caps,
        "sample_count": len(rows),
        "source": source,
        "timeout_seconds": timeout,
        "max_tokens": max_tokens,
        "opencode_agent": opencode_agent,
        "opencode_provider": opencode_provider,
        "redact_inputs": redact_inputs,
        "samples": [
            {
                "email_id": row["id"],
                "email_date": row.get("date") or "",
                "sender": row.get("sender") or "",
                "subject": row.get("subject") or "",
                "subject_chars": len(row.get("subject") or ""),
                "snippet_chars": len(row.get("snippet") or ""),
                "body_chars": len(row.get("body_text") or ""),
                "body_excerpt": _email_excerpt(
                    row.get("body_text") or row.get("snippet") or "",
                    redact=redact_inputs,
                ),
            }
            for row in rows
        ],
    }

    try:
        with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()
            csv_file.flush()

            for model in benchmark_models:
                if progress and not use_progress_bar:
                    _benchmark_progress_log(
                        f"Starting benchmark model={model}",
                        completed_requests,
                        total_requests,
                        started_at,
                        force=True,
                    )
                warm_failed = False
                warm_error = ""
                try:
                    warm = rows[0]
                    warm_prompt = f"Sender: {warm['sender']}\nSubject: {warm['subject']}\nDate: {warm['date']}\nBody:\n{warm['body_text'][:500]}"
                    if backend == "openai":
                        if client is None:
                            raise RuntimeError("OpenAI client was not initialized")
                        client.chat.completions.create(
                            model=model,
                            messages=[
                                {"role": "system", "content": CLASSIFICATION_SYSTEM_PROMPT},
                                {"role": "user", "content": warm_prompt},
                            ],
                            temperature=0.1,
                            timeout=timeout,
                            max_tokens=max_tokens,
                            extra_body={"reasoning_effort": "none"},
                        )
                    else:
                        _opencode_classify(model, warm_prompt, timeout, opencode_agent)
                except Exception as exc:
                    warm_failed = True
                    warm_error = f"{type(exc).__name__}: {str(exc)[:500]}"

                context = (
                    _model_context(base_url, model) if backend == "openai" else {"state": "cli"}
                )
                model_contexts[model] = context

                if warm_failed:
                    for cap in caps:
                        for row in rows:
                            writer.writerow(
                                _csv_row(
                                    run_id,
                                    server,
                                    model,
                                    context,
                                    row,
                                    cap,
                                    status="WARMUP_ERROR",
                                    error=warm_error,
                                )
                            )
                    csv_file.flush()
                    _write_markdown(
                        csv_path, md_path, metadata, benchmark_models, caps, model_contexts
                    )
                    completed_requests += len(rows) * len(caps)
                    if progress_bar and progress_task is not None:
                        progress_bar.update(progress_task, advance=len(rows) * len(caps))
                    continue

                for cap in caps:
                    for row in rows:
                        prompt = (
                            f"Sender: {row['sender']}\nSubject: {row['subject']}\n"
                            f"Date: {row['date']}\nBody:\n{(row['body_text'] or '')[:cap]}"
                        )
                        started = time.perf_counter()
                        status = "OK"
                        error = ""
                        raw_output = ""
                        parsed_category = ""
                        parsed_confidence = ""
                        parsed_suggested_category = ""
                        parsed_summary = ""
                        parsed_action = ""
                        parse_valid = False
                        try:
                            if backend == "openai":
                                if client is None:
                                    raise RuntimeError("OpenAI client was not initialized")
                                completion = client.chat.completions.create(
                                    model=model,
                                    messages=[
                                        {"role": "system", "content": CLASSIFICATION_SYSTEM_PROMPT},
                                        {"role": "user", "content": prompt},
                                    ],
                                    temperature=0.1,
                                    timeout=timeout,
                                    max_tokens=max_tokens,
                                    extra_body={"reasoning_effort": "none"},
                                )
                                raw_output = (completion.choices[0].message.content or "").strip()
                            else:
                                raw_output = _opencode_classify(
                                    model, prompt, timeout, opencode_agent
                                )
                            (
                                parsed_category,
                                confidence,
                                parsed_suggested_category,
                                parsed_summary,
                                parsed_action,
                            ) = parse_classification(raw_output)
                            parsed_confidence = f"{confidence:.4g}"
                            parse_valid = bool(
                                parsed_suggested_category
                                or parsed_summary
                                or parsed_action
                                or confidence > 0
                            )
                        except Exception as exc:
                            status = "ERROR"
                            error = f"{type(exc).__name__}: {str(exc)[:500]}"
                        elapsed = time.perf_counter() - started
                        writer.writerow(
                            _csv_row(
                                run_id,
                                server,
                                model,
                                context,
                                row,
                                cap,
                                prompt_chars=len(prompt),
                                elapsed_seconds=f"{elapsed:.3f}",
                                raw_output_chars=len(raw_output),
                                status=status,
                                error=error,
                                raw_output=raw_output,
                                parsed_category=parsed_category,
                                parsed_confidence=parsed_confidence,
                                parsed_suggested_category=parsed_suggested_category,
                                parsed_summary=parsed_summary,
                                parsed_action=parsed_action,
                                parse_valid=str(parse_valid).lower(),
                            )
                        )
                        csv_file.flush()
                        _write_markdown(
                            csv_path, md_path, metadata, benchmark_models, caps, model_contexts
                        )
                        completed_requests += 1
                        if progress_bar and progress_task is not None:
                            progress_bar.update(
                                progress_task,
                                advance=1,
                                description=f"{model} cap={cap}",
                            )
                        elif progress:
                            now = time.time()
                            if now - last_log_at >= 30 or completed_requests == total_requests:
                                _benchmark_progress_log(
                                    "Benchmark progress",
                                    completed_requests,
                                    total_requests,
                                    started_at,
                                )
                                last_log_at = now
            csv_file.flush()
            _write_markdown(csv_path, md_path, metadata, benchmark_models, caps, model_contexts)
    finally:
        if progress_bar:
            progress_bar.stop()

    return {
        "csv_path": csv_path,
        "markdown_path": md_path,
        "rows": len(rows) * len(caps) * len(benchmark_models),
    }


def _benchmark_progress_log(
    message: str,
    completed: int,
    total: int,
    started_at: float,
    force: bool = False,
) -> None:
    if not force and sys.stdout.isatty():
        return
    elapsed = max(time.time() - started_at, 0.001)
    rate = completed / elapsed
    print(
        f"[{time.strftime('%H:%M:%S')}] {message}: {completed}/{total} ({rate:.2f}/s)",
        flush=True,
    )


def _csv_row(
    run_id: str,
    server: dict[str, Any],
    model: str,
    context: dict[str, Any],
    row: dict[str, Any],
    cap: int,
    prompt_chars: int | str = "",
    elapsed_seconds: str = "",
    raw_output_chars: int | str = "",
    status: str = "OK",
    error: str = "",
    raw_output: str = "",
    parsed_category: str = "",
    parsed_confidence: str = "",
    parsed_suggested_category: str = "",
    parsed_summary: str = "",
    parsed_action: str = "",
    parse_valid: str = "false",
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "server_name": server["name"],
        "server_url": server["url"],
        "model": model,
        "loaded_context_length": context.get("loaded_context_length"),
        "max_context_length": context.get("max_context_length"),
        "email_id": row["id"],
        "email_date": row.get("date") or "",
        "subject_chars": len(row.get("subject") or ""),
        "snippet_chars": len(row.get("snippet") or ""),
        "body_chars": len(row.get("body_text") or ""),
        "body_cap_chars": cap,
        "prompt_chars": prompt_chars,
        "elapsed_seconds": elapsed_seconds,
        "raw_output_chars": raw_output_chars,
        "status": status,
        "error": error,
        "raw_output": raw_output,
        "parsed_category": parsed_category,
        "parsed_confidence": parsed_confidence,
        "parsed_suggested_category": parsed_suggested_category,
        "parsed_summary": parsed_summary,
        "parsed_action": parsed_action,
        "parse_valid": parse_valid,
    }
