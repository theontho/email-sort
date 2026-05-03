# Email Sorter

A comprehensive, automated email sorting and classification pipeline designed to handle gigabytes of mail offline using heuristics and local language models via OpenAI-compatible APIs (like LM-Studio, Ollama, or vLLM) with multiple llm machine runners.

## Prerequisites

1. [uv](https://github.com/astral-sh/uv) installed.
2. A local or remote LLM server exposing an OpenAI-compatible API (e.g., LM-Studio running locally).
3. A Google Takeout `.mbox` export, OR a Fastmail API token.
4. Optional for browser-based unsubscribe fallback: Playwright Chromium installed with `uv run playwright install chromium`.

## Installation

You can install the tool globally using `uv`:

```bash
uv tool install .
```

This will expose the `email-sort` CLI command.

Alternatively, you can run the tool directly from the source using `uv run`:

```bash
uv run email-sort [command]
```


## Pipeline Steps

### 1. Configure the Pipeline

Copy the example configuration to set up your tokens and server details.

```bash
cp conf.example.toml conf.toml
```

Edit `conf.toml` to include your Fastmail token (if using Fastmail) and your LLM server details (URLs, worker counts, optional names, and model name).

### 2. Initialize the Database

Create the SQLite database to store all email metadata and classification state.

```bash
email-sort init-db
```

### 3. Ingest Mail

**For MBOX (e.g., Google Takeout):**
Extract your `.mbox` file and run:
```bash
email-sort ingest-mbox path/to/your/Takeout.mbox
```

**For Fastmail:**
Ensure your `fastmail_token` is set in `conf.toml` and run:
```bash
email-sort ingest-fastmail
```

### 4. Run Fast Heuristics

This step uses `fasttext` to detect foreign languages instantly and flags obvious automated/non-personal emails based on recipients. It will download a lightweight language model the first time it runs.

```bash
email-sort heuristics
```

### 5. Run LLM Classification

Make sure your LLM servers are running. This step uses your configured models to categorize all the emails that couldn't be instantly classified by the heuristics.

```bash
email-sort classify
```

You can monitor the progress of the classification in another terminal:
```bash
email-sort watch
```

The script will automatically load your configuration from `conf.toml` and distribute tasks across all enabled servers in your cluster.

### 6. Export Results

Generate CSV files containing actionable insights (e.g., senders to ban, and subscriptions to cancel).

```bash
email-sort export
```

This produces `ban_list.csv` and `unsubscribe_list.csv` in your current directory.
