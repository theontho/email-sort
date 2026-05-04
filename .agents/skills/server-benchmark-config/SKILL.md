---
name: server-benchmark-config
description: Run email-sort server classification benchmarks, choose the best model/body cap configuration, and update the local conf.toml with the evaluated winner.
compatibility: opencode
metadata:
  workflow: email-sort-benchmark
  requires: uv
---

## Purpose

Use this skill when the user wants an autonomous email-sort benchmark pass that evaluates one or more configured LLM servers, determines the best classification model and body-character cap, and updates the user's local `conf.toml` with the evaluated winner.

This skill is project-specific for the `email-sort` repository.

## Safety Requirements

- Only update the user's local runtime config file, usually `conf.toml` from `email_sort.config.get_config_path()`.
- Treat `conf.toml` as secret-bearing because it may contain tokens. Never print token values, copy them into reports, or commit the file.
- Preserve all existing unrelated config keys, comments when practical, secrets, server URLs, API keys, IMAP settings, and SMTP settings.
- Only change model/body-cap settings that are directly justified by the benchmark result.
- Do not modify `conf.example.toml`, source defaults, tests, or committed files unless the user explicitly asks for a code change.
- Do not commit local config changes unless the user explicitly asks, and warn before committing any file that may contain secrets.
- If the benchmark fails, produces too few valid rows, or has no clear winner, do not update config. Report the blocker and the benchmark artifacts.

## Initial Discovery

1. Confirm the current repository and config path:
   - Run `git status --short --branch`.
   - Run `uv run email-sort precheck` to verify config, database, language model, and server DNS health.
   - Determine the config path with `python -c 'from email_sort.config import get_config_path; print(get_config_path())'` or by reading `uv run email-sort precheck` output.
2. Inspect available configured servers:
   - Read `conf.toml` carefully but do not reveal secret values.
   - Identify enabled `[[servers]]` entries and any existing per-server `model` overrides.
3. Determine benchmark scope:
   - If the user named a server, benchmark that server.
   - If exactly one enabled server exists, benchmark that server.
   - If multiple enabled servers exist and the user did not choose one, benchmark all enabled servers unless doing so would be obviously too expensive; otherwise ask one short clarification question.

## Benchmark Commands

Prefer the project's built-in benchmark CLI.

List models for a server:

```sh
uv run email-sort benchmark-models SERVER_NAME
```

Run the standard classification benchmark:

```sh
uv run email-sort benchmark-classification SERVER_NAME --caps 500,1000,2000,4000,8000,12000 --samples 3
```

For a faster initial pass, use `--samples 2 --caps 1000,4000,8000`, then follow up with the standard benchmark for finalist models.

For finalist confirmation, benchmark only the top candidates:

```sh
uv run email-sort benchmark-classification SERVER_NAME --models MODEL_A,MODEL_B --caps 2000,4000,8000,12000 --samples 3
```

Use `--source SOURCE` only if the user asks to optimize for one ingested source. Use `--redact-inputs` only if the user requests redacted benchmark reports; unredacted local reports are better for quality review.

## Evaluation Rules

Evaluate CSV timing data and Markdown output quality together. Do not pick by speed alone.

Reject a model/cap candidate when any of these are true:

- It has warmup errors, API errors, or missing rows.
- It does not parse every sampled email at that cap.
- It emits reasoning text, special-token garbage, blank required fields, or malformed five-field output.
- It repeatedly misclassifies obvious benchmark emails compared with the visible email inputs.
- It produces empty or nonsensical `summary`, `suggested_category`, or `action` fields.

Prefer candidates with these properties:

- `parse_valid` is true for every sampled email at the chosen cap.
- Categories and actions are semantically stable across caps.
- Summaries are short, specific, and do not contain commas that break parsing.
- Median latency is low enough for bulk classification.
- Higher caps provide real quality improvement, not just longer generic summaries.

Use this scoring order:

1. Parse reliability and clean format.
2. Semantic quality of category, action, title, and summary.
3. Stability across samples and caps.
4. Median and max latency.
5. Lower cost/body cap when quality is equivalent.

For `classification_body_chars`, choose the smallest cap that preserves quality. In this repo, `4000` is a strong default baseline; only choose lower if quality is equivalent, and only choose higher if the Markdown outputs show a meaningful quality gain.

## Config Update Policy

Update `conf.toml` minimally after selecting a winner.

When benchmarking exactly one server and multiple enabled servers exist:

- Set or update that server's `model = "WINNING_MODEL"` in its matching `[[servers]]` block.
- Update `[general].classification_body_chars = WINNING_CAP`.
- Do not change `[general].model_name` unless the user explicitly wants the selected model to become the global default for every server.

When benchmarking all enabled servers or a single-server config:

- Set `[general].model_name = "WINNING_MODEL"`.
- Set `[general].classification_body_chars = WINNING_CAP`.
- If per-server `model` overrides exist and conflict with the intended global winner, stop and ask before removing or changing them.

When separate servers have different winners:

- Set each server's `model = "WINNING_MODEL_FOR_THAT_SERVER"`.
- Set `[general].classification_body_chars` to the best shared cap unless the user asks for server-specific behavior. The current app supports body cap globally, not per server.

Preserve formatting as much as possible. Avoid wholesale TOML rewrites if a targeted edit can safely update the needed lines.

## Verification

After editing config:

1. Run `uv run email-sort precheck`.
2. Run a smoke benchmark on the selected config:

```sh
uv run email-sort benchmark-classification SERVER_NAME --models WINNING_MODEL --caps WINNING_CAP --samples 1
```

3. If the smoke benchmark fails or produces invalid parsing, revert only your config change and report the failure. Do not revert unrelated user edits.

## Final Report

Report concisely:

- Benchmark server(s), CSV path(s), and Markdown path(s).
- Winning model and `classification_body_chars`.
- Why the winner beat the alternatives, including parse rate, latency, and quality notes.
- Exact config keys changed, without exposing secrets.
- Verification commands run and their result.
- Any caveats, such as small sample size or excluded models due to warmup/load failures.
