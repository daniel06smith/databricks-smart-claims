# Databricks notebook source
# MAGIC %md
# MAGIC # Claims data quality scorecard
# MAGIC
# MAGIC Checks every claim in `02_silver.claim` (joined to its policy) against a set of data quality rules,
# MAGIC records which rules each claim passes or fails and why, and summarizes pass rates per rule.
# MAGIC
# MAGIC Outputs (all in `smart_claims_dev.04_quality`):
# MAGIC - `dq_rules`: the rules themselves, stored as data
# MAGIC - `dq_results`: one row per claim per rule, with pass/fail and the failure reason
# MAGIC - `dq_claim_status`: one row per claim, with the number of failed rules and their names
# MAGIC - `dq_scorecard`: one row per rule, with pass/fail counts and pass rate

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "smart_claims_dev"
SILVER = f"{CATALOG}.02_silver"
QUALITY = f"{CATALOG}.04_quality"
EXPORT_DIR = f"/Volumes/{CATALOG}/04_quality/exports"

# Each rule: id, name, quality dimension, columns it needs, a SQL check that is TRUE when the record is good,
# and the reason shown when it fails. Checks use try_cast so malformed text fails the rule instead of crashing.
RULES = [
    ("R01", "claim_no_present", "Completeness", ["claim_no"],
     "claim_no IS NOT NULL AND trim(claim_no) <> ''",
     "Claim number is missing"),
    ("R02", "claim_no_unique", "Uniqueness", ["claim_no"],
     "claim_no_count = 1",
     "Claim number appears more than once"),
    ("R03", "policy_exists", "Referential integrity", ["policy_no"],
     "policy_found",
     "Claim's policy number has no matching policy record"),
    ("R04", "incident_date_valid", "Validity", ["incident_date"],
     "try_cast(incident_date AS DATE) IS NOT NULL",
     "Incident date is missing or not a valid date"),
    ("R05", "claim_date_valid", "Validity", ["claim_date"],
     "try_cast(claim_date AS DATE) IS NOT NULL",
     "Claim date is missing or not a valid date"),
    ("R06", "incident_before_claim", "Consistency", ["incident_date", "claim_date"],
     "try_cast(incident_date AS DATE) <= try_cast(claim_date AS DATE)",
     "Incident date is after the claim date (or a date is missing)"),
    ("R07", "claim_within_policy_period", "Consistency", ["claim_date", "pol_eff_date", "pol_expiry_date"],
     "try_cast(claim_date AS DATE) BETWEEN try_cast(pol_eff_date AS DATE) AND try_cast(pol_expiry_date AS DATE)",
     "Claim date falls outside the policy's effective period (or a date is missing)"),
    ("R08", "incident_hour_in_range", "Validity", ["incident_hour"],
     "try_cast(incident_hour AS DOUBLE) BETWEEN 0 AND 23",
     "Incident hour is missing or outside 0-23"),
    ("R09", "driver_age_plausible", "Validity", ["driver_age"],
     "try_cast(driver_age AS DOUBLE) BETWEEN 16 AND 100",
     "Driver age is missing or outside 16-100"),
    ("R10", "license_before_incident", "Consistency", ["license_issue_date", "incident_date"],
     "try_cast(license_issue_date AS DATE) <= try_cast(incident_date AS DATE)",
     "Licence issue date is after the incident (or a date is missing)"),
    ("R11", "total_amount_valid", "Validity", ["total"],
     "try_cast(total AS DOUBLE) >= 0",
     "Total claim amount is missing, not numeric, or negative"),
    ("R12", "amount_components_add_up", "Consistency", ["total", "injury", "property", "vehicle"],
     "abs(try_cast(total AS DOUBLE) - (coalesce(try_cast(injury AS DOUBLE), 0) "
     "+ coalesce(try_cast(property AS DOUBLE), 0) + coalesce(try_cast(vehicle AS DOUBLE), 0))) <= 1",
     "Injury + property + vehicle amounts do not add up to the total"),
    ("R13", "severity_recognized", "Validity", ["incident_severity"],
     "incident_severity IN ('Trivial Damage', 'Minor Damage', 'Major Damage', 'Total Loss')",
     "Incident severity is missing or not a recognized category"),
]


def build_base(claim_df, policy_df):
    """Claims joined to the policy columns the rules need, plus helper columns."""
    pol_cols = [c for c in ["pol_eff_date", "pol_expiry_date"] if c in policy_df.columns]
    policy = (policy_df.select("policy_no", *pol_cols)
              .dropDuplicates(["policy_no"])
              .withColumn("policy_found", F.lit(True)))
    return (claim_df
            .join(policy, on="policy_no", how="left")
            .withColumn("policy_found", F.coalesce(F.col("policy_found"), F.lit(False)))
            .withColumn("claim_no_count", F.count(F.lit(1)).over(Window.partitionBy("claim_no"))))


def active_rules(columns):
    """Rules whose columns all exist; the rest are reported and skipped instead of failing the run."""
    kept, skipped = [], []
    for rule in RULES:
        missing = [c for c in rule[3] if c not in columns]
        (skipped if missing else kept).append((rule, missing))
    return [r for r, _ in kept], [(r[0], r[1], m) for r, m in skipped]


def build_results(base, rules):
    """One row per claim per rule. A check that evaluates to NULL counts as a failure."""
    checks = F.array(*[
        F.struct(
            F.lit(rid).alias("rule_id"),
            F.lit(name).alias("rule_name"),
            F.lit(dim).alias("dimension"),
            F.coalesce(F.expr(check), F.lit(False)).alias("passed"),
            F.lit(reason).alias("reason"),
        )
        for rid, name, dim, _, check, reason in rules
    ])
    return (base
            .select("claim_no", "policy_no", F.explode(checks).alias("r"))
            .select("claim_no", "policy_no", "r.rule_id", "r.rule_name", "r.dimension", "r.passed",
                    F.when(~F.col("r.passed"), F.col("r.reason")).alias("failure_reason")))


def build_claim_status(results):
    return (results.groupBy("claim_no", "policy_no")
            .agg(F.sum(F.when(~F.col("passed"), 1).otherwise(0)).alias("rules_failed"),
                 F.count(F.lit(1)).alias("rules_checked"),
                 F.concat_ws("; ", F.sort_array(F.collect_list(
                     F.when(~F.col("passed"), F.col("rule_name"))))).alias("failed_rules"))
            .withColumn("record_status",
                        F.when(F.col("rules_failed") == 0, "Pass").otherwise("Fail")))


def build_scorecard(results):
    return (results.groupBy("rule_id", "rule_name", "dimension")
            .agg(F.count(F.lit(1)).alias("records_checked"),
                 F.sum(F.col("passed").cast("int")).alias("records_passed"))
            .withColumn("records_failed", F.col("records_checked") - F.col("records_passed"))
            .withColumn("pass_rate", F.round(F.col("records_passed") / F.col("records_checked"), 4))
            .orderBy("rule_id"))

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {QUALITY}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {QUALITY}.exports")

base = build_base(spark.table(f"{SILVER}.claim"), spark.table(f"{SILVER}.policy"))
rules, skipped = active_rules(base.columns)
for rid, name, missing in skipped:
    print(f"Skipped {rid} {name}: missing column(s) {missing}")
print(f"Running {len(rules)} rules on {base.count():,} claims")

# COMMAND ----------

rules_df = spark.createDataFrame(
    [(rid, name, dim, check, reason) for rid, name, dim, _, check, reason in rules],
    "rule_id string, rule_name string, dimension string, check_sql string, failure_reason string")

results = build_results(base, rules)
claim_status = build_claim_status(results)
scorecard = build_scorecard(results)

for name, df in [("dq_rules", rules_df), ("dq_results", results),
                 ("dq_claim_status", claim_status), ("dq_scorecard", scorecard)]:
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{QUALITY}.{name}")

display(spark.table(f"{QUALITY}.dq_scorecard"))

# COMMAND ----------

# Summary: how many claims pass every rule
display(spark.table(f"{QUALITY}.dq_claim_status")
        .groupBy("record_status").count())

# COMMAND ----------

# Export CSVs for Power BI. Download them from Catalog > smart_claims_dev > 04_quality > exports.
for name in ["dq_rules", "dq_scorecard", "dq_claim_status", "dq_results"]:
    spark.table(f"{QUALITY}.{name}").toPandas().to_csv(f"{EXPORT_DIR}/{name}.csv", index=False)
    print("Wrote", f"{EXPORT_DIR}/{name}.csv")
