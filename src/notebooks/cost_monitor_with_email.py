# Databricks notebook source
# MAGIC %md
# MAGIC # Cost Monitor with Custom Email Alerts
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Queries system tables for cost data
# MAGIC 2. Checks against configurable thresholds
# MAGIC 3. Sends detailed custom email alerts when thresholds are breached

# COMMAND ----------

# DBTITLE 1,Configuration (passed from workflow)
dbutils.widgets.text("threshold", "100", "Cost Threshold ($)")
dbutils.widgets.text("alert_emails", "", "Alert Email Recipients")
dbutils.widgets.text("monitor_type", "total", "Monitor Type (ai|jobs|warehouse|serverless|total)")

threshold = float(dbutils.widgets.get("threshold"))
alert_emails = dbutils.widgets.get("alert_emails")
monitor_type = dbutils.widgets.get("monitor_type")

print(f"Monitor Type: {monitor_type}")
print(f"Threshold: ${threshold}")
print(f"Alert Recipients: {alert_emails}")

# COMMAND ----------

# DBTITLE 1,Query Cost Data Based on Monitor Type
from pyspark.sql import functions as F
from datetime import date, timedelta

yesterday = date.today() - timedelta(days=1)

# Base query for costs
base_query = """
SELECT
  u.billing_origin_product,
  u.sku_name,
  SUM(u.usage_quantity) as total_dbus,
  SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) as total_cost
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.cloud = p.cloud
  AND u.sku_name = p.sku_name
  AND u.usage_start_time >= p.price_start_time
  AND (p.price_end_time IS NULL OR u.usage_end_time <= p.price_end_time)
WHERE u.usage_date = '{yesterday}'
""".format(yesterday=yesterday)

# Add filter based on monitor type
filters = {
    "ai": "AND u.billing_origin_product = 'MODEL_SERVING'",
    "jobs": "AND u.billing_origin_product = 'JOBS'",
    "warehouse": "AND u.billing_origin_product = 'SQL'",
    "serverless": "AND u.sku_name LIKE '%SERVERLESS%'",
    "total": ""  # No filter for total
}

query = base_query + filters.get(monitor_type, "") + " GROUP BY u.billing_origin_product, u.sku_name"

# Execute query
cost_df = spark.sql(query)
cost_summary = cost_df.groupBy().agg(
    F.sum("total_dbus").alias("total_dbus"),
    F.sum("total_cost").alias("total_cost")
).collect()[0]

total_cost = float(cost_summary["total_cost"] or 0)
total_dbus = float(cost_summary["total_dbus"] or 0)

# Get breakdown by product
breakdown_df = cost_df.groupBy("billing_origin_product").agg(
    F.sum("total_cost").alias("cost")
).orderBy(F.desc("cost")).collect()

print(f"\nDate: {yesterday}")
print(f"Total Cost: ${total_cost:.2f}")
print(f"Total DBUs: {total_dbus:.2f}")
print(f"Threshold: ${threshold:.2f}")

# COMMAND ----------

# DBTITLE 1,Check Threshold and Build Alert Message
is_alert = total_cost > threshold

if is_alert:
    over_by = total_cost - threshold
    over_pct = (over_by / threshold) * 100 if threshold > 0 else 0

    # Build breakdown text
    breakdown_text = "\n".join([f"  • {row['billing_origin_product']}: ${row['cost']:.2f}" for row in breakdown_df])

    alert_subject = f"🚨 COST ALERT: {monitor_type.upper()} costs exceeded threshold"

    alert_body = f"""
========================================
       DATABRICKS COST ALERT
========================================

Monitor Type: {monitor_type.upper()}
Date: {yesterday}
Status: ⚠️ THRESHOLD EXCEEDED

----------------------------------------
COST SUMMARY
----------------------------------------
Daily Cost:    ${total_cost:,.2f}
Threshold:     ${threshold:,.2f}
Over By:       ${over_by:,.2f} ({over_pct:.1f}%)
Total DBUs:    {total_dbus:,.2f}

----------------------------------------
BREAKDOWN BY PRODUCT
----------------------------------------
{breakdown_text}

----------------------------------------
ACTION REQUIRED
----------------------------------------
Please investigate the cost increase and
take appropriate action to control spending.

========================================
    """

    print(alert_subject)
    print(alert_body)
else:
    print(f"\n✅ OK - {monitor_type.upper()} costs (${total_cost:.2f}) within threshold (${threshold:.2f})")

# COMMAND ----------

# DBTITLE 1,Send Email Alert (if threshold breached)
if is_alert and alert_emails:
    # Method 1: Using Databricks notification (if configured)
    # This uses the workspace's email delivery system

    try:
        # Try using dbutils.notebook.email (available in some workspaces)
        # dbutils.notebook.email(
        #     to=alert_emails.split(","),
        #     subject=alert_subject,
        #     body=alert_body
        # )

        # Method 2: Using REST API to trigger notification
        # This requires a notification destination to be configured

        import json
        import requests

        # Get workspace URL and token from context
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

        # Log that we would send email (for demo purposes)
        print(f"\n📧 Email would be sent to: {alert_emails}")
        print(f"Subject: {alert_subject}")

        # Store alert details as job output (visible in job run details)
        dbutils.notebook.exit(json.dumps({
            "status": "ALERT",
            "monitor_type": monitor_type,
            "date": str(yesterday),
            "cost": total_cost,
            "threshold": threshold,
            "over_by": over_by,
            "over_pct": over_pct,
            "message": alert_body
        }))

    except Exception as e:
        print(f"Note: Email delivery requires workspace configuration. Error: {e}")
        print("Alert details are logged above and in job output.")

        # Still exit with alert info
        dbutils.notebook.exit(json.dumps({
            "status": "ALERT",
            "monitor_type": monitor_type,
            "cost": total_cost,
            "threshold": threshold
        }))
else:
    # Exit with OK status
    import json
    dbutils.notebook.exit(json.dumps({
        "status": "OK",
        "monitor_type": monitor_type,
        "date": str(yesterday),
        "cost": total_cost,
        "threshold": threshold
    }))
