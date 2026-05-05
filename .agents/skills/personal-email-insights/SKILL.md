---
name: personal-email-insights
description: Analyze classified personal email data, produce privacy-safe insights, and write gitignored Markdown cleanup recommendations for unsubscribe, filtering, and retention decisions.
compatibility: opencode
metadata:
  workflow: email-sort-post-classification-insights
  requires: uv
---

## Purpose

Use this skill after `email-sort` has ingested, heuristiced, and classified a user's email archive. The goal is to turn the classified SQLite database into practical, personal, privacy-conscious recommendations:

- What can be learned from the mailbox.
- Which senders are safe unsubscribe candidates.
- Which services need preference-center cleanup instead of blanket unsubscribe.
- Which categories should be kept, archived, filtered, or blocked.
- Which reports should be written locally under a gitignored path.

This skill is project-specific for the `email-sort` repository.

## Safety And Privacy Requirements

- Treat email data, subjects, sender addresses, unsubscribe URLs, and config files as private personal data.
- Do not reveal secrets from `conf.toml`, API tokens, SMTP passwords, IMAP passwords, or unsubscribe tokens.
- Prefer aggregate counts, sender/domain summaries, and category-level recommendations over quoting message bodies.
- Do not print raw unsubscribe URLs unless the user explicitly needs them; they often contain personal tokens.
- Do not run unsubscribe execution by default. Use dry runs and reports only unless the user explicitly asks to execute.
- Do not recommend domain-level blocking for shared or important domains such as Gmail, Google, Apple, banks, payment processors, shippers, universities, employers, or broad mailbox providers.
- Never delete emails, clear mailboxes, upload Sieve scripts, or run destructive mailbox operations unless the user explicitly asks and the operation is reversible or confirmed.
- Store personal reports under a gitignored directory, preferably `data/recommendations/` or `out/personal-email-insights/`.
- If a report includes personally identifying data, keep it in a gitignored path and mention that it should not be committed.

## Initial Discovery

1. Confirm repository and ignore status:

```sh
git status --short --branch
```

2. Verify local data/report directories are ignored by reading `.gitignore`.
3. Confirm the database path. Usually it is `data/emails.db`, but the app may resolve another path via `EMAIL_SORT_DB` or `conf.toml`.
4. Check classification state with:

```sh
uv run email-sort stats
```

5. If sender reputation may be stale or absent, run:

```sh
uv run email-sort analyze-senders
```

6. Generate standard reports:

```sh
uv run email-sort export all
```

7. Preview unsubscribe candidates without acting:

```sh
uv run email-sort unsubscribe --dry-run
```

## Core Queries

Use SQLite aggregate queries against the email database. Prefer `PRAGMA busy_timeout=30000;` because classification or ingestion may leave the database briefly locked.

Overall mailbox summary:

```sh
sqlite3 -header -column "data/emails.db" "
PRAGMA busy_timeout=30000;
SELECT COUNT(*) AS total,
       COUNT(DISTINCT source) AS sources,
       COUNT(DISTINCT sender) AS senders,
       COUNT(DISTINCT sender_domain) AS domains,
       SUM(CASE WHEN COALESCE(category, rule_category, heuristic_category, '') != '' THEN 1 ELSE 0 END) AS categorized,
       SUM(CASE WHEN TRIM(COALESCE(list_unsubscribe, '')) != ''
                 OR TRIM(COALESCE(body_unsubscribe_links, '')) NOT IN ('', '[]')
                THEN 1 ELSE 0 END) AS nonempty_unsub,
       SUM(CASE WHEN is_digest = 1 THEN 1 ELSE 0 END) AS digests,
       SUM(CASE WHEN is_duplicate = 1 THEN 1 ELSE 0 END) AS duplicates
FROM emails;
"
```

Category distribution and unsubscribe coverage:

```sh
sqlite3 -header -column "data/emails.db" "
PRAGMA busy_timeout=30000;
WITH e AS (
    SELECT COALESCE(
               CASE WHEN rule_source = 'manual-correction' THEN rule_category END,
               category, rule_category, heuristic_category, 'Unclassified'
           ) AS cat,
           sender,
           sender_domain,
           list_unsubscribe,
           body_unsubscribe_links
    FROM emails
)
SELECT cat AS category,
       COUNT(*) AS emails,
       COUNT(DISTINCT sender) AS senders,
       COUNT(DISTINCT sender_domain) AS domains,
       SUM(CASE WHEN TRIM(COALESCE(list_unsubscribe, '')) != ''
                 OR TRIM(COALESCE(body_unsubscribe_links, '')) NOT IN ('', '[]')
                THEN 1 ELSE 0 END) AS with_unsub
FROM e
GROUP BY cat
ORDER BY emails DESC;
"
```

Removable volume:

```sh
sqlite3 -header -column "data/emails.db" "
PRAGMA busy_timeout=30000;
WITH e AS (
    SELECT sender_domain,
           COALESCE(
               CASE WHEN rule_source = 'manual-correction' THEN rule_category END,
               category, rule_category, heuristic_category, 'Unclassified'
           ) AS cat,
           list_unsubscribe,
           body_unsubscribe_links
    FROM emails
)
SELECT COUNT(DISTINCT sender_domain) AS domains,
       SUM(cat IN ('Promotional','Newsletter','Shopping','Social','Tech','Health','Spam')) AS removable_emails,
       SUM(CASE WHEN cat IN ('Promotional','Newsletter','Shopping','Social','Tech','Health','Spam')
                 AND (TRIM(COALESCE(list_unsubscribe, '')) != ''
                      OR TRIM(COALESCE(body_unsubscribe_links, '')) NOT IN ('', '[]'))
                THEN 1 ELSE 0 END) AS removable_with_unsub
FROM e;
"
```

Lower-risk unsubscribe targets:

```sh
sqlite3 -header -column "data/emails.db" "
PRAGMA busy_timeout=30000;
WITH e AS (
    SELECT sender,
           sender_domain,
           date,
           COALESCE(
               CASE WHEN rule_source = 'manual-correction' THEN rule_category END,
               category, rule_category, heuristic_category, 'Unclassified'
           ) AS cat,
           list_unsubscribe,
           body_unsubscribe_links
    FROM emails
),
d AS (
    SELECT sender_domain,
           SUM(cat IN ('Personal','Finance','Security','Shipping','Travel','Work','Home','Education')) AS important,
           SUM(cat = 'Automated') AS automated
    FROM e
    GROUP BY sender_domain
)
SELECT e.sender,
       e.sender_domain,
       e.cat AS category,
       COUNT(*) AS emails,
       MAX(e.date) AS last_seen,
       SUM(CASE WHEN TRIM(COALESCE(e.list_unsubscribe, '')) != ''
                 OR TRIM(COALESCE(e.body_unsubscribe_links, '')) NOT IN ('', '[]')
                THEN 1 ELSE 0 END) AS with_unsub
FROM e
JOIN d ON d.sender_domain = e.sender_domain
WHERE e.cat IN ('Promotional','Newsletter','Shopping','Social','Tech','Health','Spam')
  AND d.important = 0
  AND d.automated = 0
GROUP BY e.sender, e.sender_domain, e.cat
HAVING emails >= 50 AND with_unsub > 0
ORDER BY emails DESC
LIMIT 50;
"
```

Mixed domains requiring preference centers:

```sh
sqlite3 -header -column "data/emails.db" "
PRAGMA busy_timeout=30000;
WITH e AS (
    SELECT sender_domain,
           COALESCE(
               CASE WHEN rule_source = 'manual-correction' THEN rule_category END,
               category, rule_category, heuristic_category, 'Unclassified'
           ) AS cat
    FROM emails
)
SELECT sender_domain,
       COUNT(*) AS emails,
       SUM(cat IN ('Personal','Finance','Security','Shipping','Travel','Work','Home','Education')) AS important,
       SUM(cat IN ('Promotional','Newsletter','Shopping','Social','Tech','Health','Spam')) AS removable,
       SUM(cat = 'Automated') AS automated
FROM e
WHERE sender_domain IS NOT NULL AND sender_domain != ''
GROUP BY sender_domain
HAVING emails >= 100 AND important > 0 AND removable > 0
ORDER BY removable DESC
LIMIT 50;
"
```

Important senders to keep/filter:

```sh
sqlite3 -header -column "data/emails.db" "
PRAGMA busy_timeout=30000;
WITH e AS (
    SELECT sender,
           sender_domain,
           date,
           COALESCE(
               CASE WHEN rule_source = 'manual-correction' THEN rule_category END,
               category, rule_category, heuristic_category, 'Unclassified'
           ) AS cat
    FROM emails
)
SELECT sender,
       sender_domain,
       cat AS category,
       COUNT(*) AS emails,
       MAX(date) AS last_seen
FROM e
WHERE cat IN ('Finance','Security','Shipping','Travel')
GROUP BY sender, sender_domain, cat
HAVING emails >= 50
ORDER BY emails DESC
LIMIT 50;
"
```

Stale promotional domains that may be better archived or blocked than unsubscribed:

```sh
sqlite3 -header -column "data/emails.db" "
PRAGMA busy_timeout=30000;
WITH e AS (
    SELECT sender_domain,
           sender,
           date,
           COALESCE(
               CASE WHEN rule_source = 'manual-correction' THEN rule_category END,
               category, rule_category, heuristic_category, 'Unclassified'
           ) AS cat
    FROM emails
)
SELECT sender_domain,
       COUNT(*) AS emails,
       COUNT(DISTINCT sender) AS senders,
       MAX(date) AS last_seen
FROM e
WHERE cat IN ('Promotional','Newsletter','Shopping','Social','Tech','Health','Spam')
GROUP BY sender_domain
HAVING emails >= 100 AND last_seen < '2024-01-01'
ORDER BY emails DESC
LIMIT 50;
"
```

## Interpretation Rules

Classify recommendations into tiers.

### Tier 1: Lower-Risk Unsubscribe

Recommend sender-level unsubscribe when all are true:

- Category is Promotional, Newsletter, Spam, or clearly unwanted Shopping/Social/Tech/Health.
- Sender/domain has no detected important same-domain mail.
- Sender has unsubscribe evidence.
- Volume is meaningful, usually at least 20 to 50 emails.
- The service is not obviously a bank, identity provider, shipper, cloud provider, payment processor, school, employer, government, medical provider, or domain registrar.

### Tier 2: Preference Center

Recommend preference-center cleanup when a domain has both removable and important mail. Preserve operational messages and disable noisy categories.

Keep:

- Security alerts.
- Billing and receipts.
- Password and login alerts.
- Fraud alerts.
- Order confirmations.
- Shipping and delivery notifications.
- Critical service announcements.

Disable:

- Promotions.
- Recommendations.
- Social nudges.
- Daily or weekly digests.
- Deal alerts.
- Marketing newsletters.
- Re-engagement emails.

### Tier 3: Keep Or Filter

Recommend filters, folders, or archive rules for Finance, Security, Shipping, Travel, Work, and Personal. Do not recommend unsubscribe unless the sender is clearly a newsletter or marketing substream from an otherwise important domain.

### Tier 4: Block Or Ban

Recommend blocking only for narrow, exact senders or clearly malicious/unwanted domains. Review ban candidates carefully. Avoid broad provider or shared domains.

## Report Writing

Write Markdown reports under `data/recommendations/` by default. Create the directory if needed. Suggested files:

- `README.md`: High-level findings and report index.
- `unsubscribe-now.md`: Lower-risk unsubscribe targets.
- `mixed-domains.md`: Domains requiring preference-center cleanup.
- `keep-or-filter.md`: Important senders/categories to preserve or route.
- `cleanup-workflow.md`: Step-by-step safe cleanup plan.
- Optional `stale-promotional.md`: Old promotional domains that may not need active unsubscribe work.

Reports should include:

- The date generated.
- Aggregate counts.
- Short rationale for each tier.
- Tables with sender, domain, category, count, last seen, and unsubscribe coverage where useful.
- Clear warnings about not running broad automated unsubscribe blindly.

Avoid including:

- Message bodies.
- Full unsubscribe URLs.
- Secrets or token values.
- Long raw subject lists unless the user explicitly asks.

## Automation Guardrails

The built-in command below is safe for preview:

```sh
uv run email-sort unsubscribe --dry-run
```

The command below takes action and must not be run unless explicitly requested:

```sh
uv run email-sort unsubscribe --execute
```

If the user asks for automatic unsubscribe execution, first narrow the candidate set to exclude:

- Personal.
- Finance.
- Security.
- Shipping.
- Travel.
- Work.
- Mixed domains with important/service mail.
- Broad/shared providers.

Then run a small batch first and verify outcomes before continuing.

## Verification

After writing reports:

1. Confirm files exist under the chosen gitignored directory.
2. Confirm git sees them as ignored, for example:

```sh
git status --short --ignored data/recommendations
```

3. If standard exports were generated, mention their paths.
4. If no unsubscribe actions were taken, state that explicitly.

## Final Report

Report concisely:

- Where the Markdown recommendations were written.
- Top mailbox insights and approximate cleanup opportunity.
- The recommended first cleanup tier.
- Any safety caveats, especially mixed service domains and secret-bearing config files.
- Commands run for verification.
