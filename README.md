# Databricks Cost Monitoring with Asset Bundles

A Databricks Asset Bundle (DAB) solution for automated cost monitoring using **native SQL Alerts (V2)** and system tables.

## Overview

This bundle deploys **21 SQL Alerts** that:
- Query Databricks system tables (`system.billing.usage`, `system.billing.list_prices`, `system.lakeflow.*`)
- Calculate costs in dollars (list price) by workload, job, and warehouse
- Compare against configurable thresholds, detect growth/anomalies, and flag waste
- **Send notifications ONLY when a threshold is breached**

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Scheduled      │────▶│  SQL Query       │────▶│  Threshold      │
│  Alert          │     │  System Tables   │     │  Evaluation     │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                          │
                        ┌─────────────────────────────────┴────────┐
                        ▼                                          ▼
             ┌──────────────────┐                    ┌──────────────────┐
             │  value ≤ thresh  │                    │  value > thresh  │
             │  Status: OK      │                    │  Status: TRIGGERED│
             │  No notification │                    │  EMAIL/SLACK/etc │
             └──────────────────┘                    └──────────────────┘
```

## What's Monitored

### Daily spend by product (7) — run 13:00 UTC daily
| Alert | Filter |
|-------|--------|
| **AI / Model Serving** | `billing_origin_product = 'MODEL_SERVING'` |
| **Job Compute** | `billing_origin_product = 'JOBS'` |
| **SQL Warehouses** | `billing_origin_product = 'SQL'` |
| **Serverless Compute** | `product_features.is_serverless = true` |
| **DLT Pipelines** | `DLT` / `LAKEFLOW_CONNECT` (+ SQL with `dlt_pipeline_id`) |
| **Interactive Clusters** | `ALL_PURPOSE` with `job_id IS NULL` |
| **Total Platform** | all usage (with per-product breakdown); also runs 19:00 UTC |

### Growth & anomaly (3)
| Alert | What it checks | Schedule |
|-------|----------------|----------|
| **Anomaly Detection** | Daily total cost Z-score > 2 vs 30-day baseline | Daily 14:00 UTC |
| **Week-over-Week Growth** | Last full week vs prior week (%) | Mon 14:00 UTC |
| **30-Day Rolling Growth** | Last 30 days vs prior 30 days (%) | Mon 14:00 UTC |

### Per-job cost & health (5) — run 14:00 UTC daily
| Alert | What it checks |
|-------|----------------|
| **Expensive Job (30 days)** | Highest-cost job over 30 days |
| **Expensive Job Run** | Highest-cost single run (yesterday) |
| **Job Cost Growth** | Job spend last 7 days vs prior 7 days (%) |
| **Job Failure Rate** | Success rate below threshold (≥5 runs / 7 days) |
| **Failed Run Spend** | $ burned on `ERROR/FAILED/TIMED_OUT/CANCELED` runs (7 days) |

### Per-warehouse cost (2) — run 14:00 UTC daily
| Alert | What it checks |
|-------|----------------|
| **Single Warehouse High Cost** | Highest-cost warehouse (yesterday) |
| **Warehouse Cost Growth** | Warehouse spend last 7 days vs prior 7 days (%) |

### Waste / efficiency (1) — run 14:00 UTC daily
| Alert | What it checks |
|-------|----------------|
| **Jobs on All-Purpose Clusters** | Jobs billed on `ALL_PURPOSE` compute (~2x the jobs-compute SKU — move to job clusters) |

### Governance (2) — run 14:00 UTC daily
| Alert | What it checks |
|-------|----------------|
| **Untagged Warehouse Spend** | Highest-cost warehouse with no `custom_tags` (7 days) |
| **Untagged Job Spend** | Highest-cost job with no `custom_tags` (7 days) |

### Budget (1) — run 14:00 UTC daily
| Alert | What it checks |
|-------|----------------|
| **Budget Pacing** | Month-to-date run-rate projected to month-end vs monthly budget |

## How Alerts Work

Native SQL Alerts V2 evaluate a single output column against a threshold and notify only on transition to `TRIGGERED`. Compared to job-based monitoring (RAISE_ERROR hacks in Workflows), they offer declarative YAML config, native Email/Slack/Teams/PagerDuty destinations, and OK / TRIGGERED / UNKNOWN status tracking.

### Alert lifecycle
1. Alert runs on its schedule (UTC).
2. SQL query computes the metric (cost, %, z-score, etc.) from system tables.
3. Threshold comparison: `value ≤ threshold` → **OK** (silent); `value > threshold` → **TRIGGERED** → notification.
   (Job Failure Rate is inverted: it fires when success rate `< threshold`.)

### Notification formatting

Every alert ships a custom notification template (`custom_summary` = subject, `custom_description` = HTML body), shared across all 21 alerts via a YAML anchor (`&alert_email_body`). The body uses Alerts V2 mustache variables and renders as HTML in email:

```html
<h2>Databricks Cost Alert</h2>
<p><b>{{ALERT_NAME}}</b> changed status to <b>{{ALERT_STATUS}}</b>.</p>
<table><tbody>
  <tr><th>Metric</th><td>{{ALERT_COLUMN}}</td></tr>
  <tr><th>Current value</th><td>{{QUERY_RESULT_VALUE}}</td></tr>
  <tr><th>Condition</th><td>{{ALERT_CONDITION}} {{ALERT_THRESHOLD}}</td></tr>
</tbody></table>
<h3>Result details</h3>
{{QUERY_RESULT_TABLE}}
<p><a href="{{ALERT_URL}}">View this alert in Databricks</a></p>
```

Available variables: `ALERT_NAME`, `ALERT_STATUS`, `ALERT_CONDITION`, `ALERT_THRESHOLD`, `ALERT_COLUMN`, `ALERT_URL`, `QUERY_RESULT_VALUE`, `QUERY_RESULT_TABLE` (offending rows as an HTML table, email only). **Note:** Databricks sanitizes the body to a fixed HTML tag whitelist and strips all CSS/inline `style`/colors — for fully brand-styled emails, route a webhook destination to a job that sends via SMTP/SendGrid instead.

## Project Structure

```
cost-monitoring-dabs/
├── databricks.yml                      # Bundle config, variables, dev/prod targets
├── resources/
│   └── cost_monitoring_alerts.yml      # 21 SQL Alert (V2) definitions
├── src/
│   └── notebooks/
│       └── cost_monitor_with_email.py  # Optional: custom email notebook
├── deploy.local.sh                     # Local-only (gitignored): your real values + deploy helper
└── .gitignore
```

## Configuration

### Variables (`databricks.yml`)

`warehouse_id` and `alert_emails` are **placeholders** in the committed file — set your real values via `deploy.local.sh` (gitignored) or `--var` at deploy time. The workspace **host comes from your CLI profile**, not the YAML.

| Variable | Description | Dev | Prod |
|----------|-------------|-----|------|
| `warehouse_id` | SQL warehouse that runs the alert queries | placeholder | placeholder |
| `alert_emails` | Subscriber email | placeholder | placeholder |
| `ai_daily_cost_threshold` | AI / Model Serving ($) | 50 | 500 |
| `job_daily_cost_threshold` | Job compute ($) | 100 | 1000 |
| `warehouse_daily_cost_threshold` | SQL warehouse ($) | 75 | 800 |
| `serverless_daily_cost_threshold` | Serverless ($) | 150 | 1500 |
| `dlt_daily_cost_threshold` | DLT pipelines ($) | 100 | 500 |
| `interactive_daily_cost_threshold` | Interactive/all-purpose ($) | 250 | 1000 |
| `total_daily_cost_threshold` | Total platform ($) | 500 | 5000 |
| `wow_growth_threshold` | Week-over-week growth (%) | 30 | 25 |
| `monthly_growth_threshold` | 30-day rolling growth (%) | 40 | 30 |
| `anomaly_min_cost` | Min daily cost to consider for anomaly ($) | 25 | 50 |
| `expensive_job_threshold` | Single job, 30-day cost ($) | 1000 | 5000 |
| `expensive_job_run_threshold` | Single job run ($) | 100 | 500 |
| `job_growth_threshold` | Job 7-day cost growth (%) | 50 | 50 |
| `job_failure_rate_threshold` | Min success rate (%) | 70 | 80 |
| `failed_job_cost_threshold` | $ on failed/cancelled runs | 50 | 100 |
| `all_purpose_job_cost_threshold` | Job-on-all-purpose 30-day cost ($) | 100 | 500 |
| `monthly_budget_threshold` | Projected month-end budget ($) | 10000 | 100000 |
| `single_warehouse_cost_threshold` | Single warehouse daily ($) | 200 | 500 |
| `warehouse_growth_threshold` | Warehouse 7-day growth (%) | 30 | 30 |
| `untagged_cost_threshold` | Untagged resource cost ($) | 50 | 100 |

> Set `monthly_budget_threshold` to your **real** monthly budget — the default is a placeholder and will fire constantly on a busy workspace otherwise.

## Prerequisites

1. **Databricks CLI v0.279.0+** (required for SQL Alerts V2)
   ```bash
   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
   databricks --version
   ```
2. **System tables access** — `SELECT` on `system.billing.usage`, `system.billing.list_prices`, and `system.lakeflow.*` (jobs, job_run_timeline, job_task_run_timeline).
3. **A running SQL warehouse** to execute the alert queries.
4. **A configured CLI profile** for your workspace (provides the host).

## Quick Start

### 1. Authenticate (creates a profile that supplies the host)
```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile myws
```

### 2. Set your values
Either edit `deploy.local.sh` (gitignored) with your `WAREHOUSE_ID`, `ALERT_EMAILS`, and `PROFILE`, or pass them inline via `--var` (below).

### 3. Validate
```bash
databricks bundle validate -t dev -p myws \
  --var "warehouse_id=<id>,alert_emails=you@company.com"
```

### 4. Deploy
```bash
# Using the helper (recommended):
./deploy.local.sh dev          # or: ./deploy.local.sh prod

# Or directly:
databricks bundle deploy -t dev -p myws \
  --var "warehouse_id=<id>,alert_emails=you@company.com"
```

> **Dev pauses schedules.** The `dev` target uses `mode: development`: alerts deploy with a `[dev <user>]` name prefix and their **schedules are PAUSED**. They will not run or notify until you unpause them in the UI (or deploy to `prod`).

### 5. View alerts
Databricks UI → **SQL → Alerts**. Each shows OK / TRIGGERED / UNKNOWN.

### 6. Deploy to production (live + unpaused)
```bash
./deploy.local.sh prod
```
Prod uses `mode: production`, deploys under `${workspace.current_user.userName}`, and schedules run unpaused.

### Terraform GPG note
If `bundle deploy` fails with `error downloading Terraform ... openpgp: key expired`, point it at an already-cached Terraform binary (the `deploy.local.sh` helper does this automatically):
```bash
export DATABRICKS_TF_EXEC_PATH="$(pwd)/.databricks/bundle/dev/bin/terraform"
```

## Cost Calculation Method

Costs join usage with list prices using the Databricks-recommended pattern:

```sql
SELECT COALESCE(SUM(
  u.usage_quantity * COALESCE(p.pricing.effective_list.default, p.pricing.default, 0)
), 0) AS total_cost
FROM system.billing.usage u
INNER JOIN system.billing.list_prices p
  ON u.cloud = p.cloud
  AND u.sku_name = p.sku_name
  AND u.usage_start_time >= p.price_start_time
  AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE u.usage_date = DATE_SUB(CURRENT_DATE(), 1)
```

| Field | Description |
|-------|-------------|
| `pricing.effective_list.default` | Preferred — includes account-wide promotions (e.g. serverless discount) |
| `pricing.default` | Fallback — standard list price |

**Note:** these are **list prices**. Customer-negotiated contract discounts are not reflected in system tables.

### References
- [Monitor costs using system tables](https://docs.databricks.com/aws/en/admin/usage/system-tables)
- [Pricing system table reference](https://docs.databricks.com/aws/en/admin/system-tables/pricing)
- [Monitor job costs](https://docs.databricks.com/aws/en/admin/system-tables/jobs-cost)

## Customization

### Adjust thresholds
Edit defaults in `databricks.yml` and redeploy, or override at deploy time:
```bash
databricks bundle deploy -t dev -p myws --var "job_daily_cost_threshold=2000"
```

### Change schedules
Edit `quartz_cron_schedule` / `timezone_id` in `resources/cost_monitoring_alerts.yml`:
```yaml
schedule:
  quartz_cron_schedule: "0 0 13 * * ?"   # 13:00
  timezone_id: "UTC"
```

### Add notification destinations
Configure a destination in **Admin Console → Notification destinations**, then reference it:
```yaml
notification:
  subscriptions:
    - user_email: user@company.com
    - destination_id: abc123            # Slack/Teams/PagerDuty
```

### Add custom filters
Edit `query_text` in `resources/cost_monitoring_alerts.yml`:
```sql
AND u.workspace_id = 'xxx'
AND u.custom_tags['team'] = 'data-engineering'
```

## Troubleshooting

- **"System tables not found"** — system tables require Unity Catalog; ask your admin to enable them.
- **"Permission denied on system.billing.usage"** — `GRANT SELECT ON TABLE system.billing.usage TO \`you@company.com\`;` (and `list_prices`, `system.lakeflow.*`).
- **Alert shows UNKNOWN** — query hasn't run yet or errored. Check the warehouse is running and the query is valid.
- **No notifications** — verify status in SQL → Alerts, that `alert_emails` matches your account email, that the alert isn't paused (dev), and the threshold is appropriate.
- **CLI too old** — Alerts V2 require v0.279.0+; update via the setup-cli script above.

## System Tables Reference

| Table | Purpose |
|-------|---------|
| `system.billing.usage` | Raw usage (DBUs, timestamps, metadata) |
| `system.billing.list_prices` | SKU list pricing |
| `system.lakeflow.jobs` | Job definitions (names) |
| `system.lakeflow.job_run_timeline` | Run states & durations |
| `system.lakeflow.job_task_run_timeline` | Task/cluster attribution |

### Key columns in `system.billing.usage`
| Column | Description |
|--------|-------------|
| `usage_date` | Date of usage |
| `billing_origin_product` | JOBS, SQL, MODEL_SERVING, ALL_PURPOSE, DLT, ... |
| `sku_name` | SKU identifier |
| `usage_quantity` | Amount consumed (DBUs) |
| `product_features.is_serverless` | Boolean for serverless workloads |
| `usage_metadata` | Struct: `job_id`, `job_run_id`, `warehouse_id`, `cluster_id`, ... |
| `custom_tags` | User/resource tags for cost attribution |

## License

Internal use only.
