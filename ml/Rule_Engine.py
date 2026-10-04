# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE smart_claims_dev.03_gold.customer_claim_policy_telematics_predicted AS
# MAGIC   SELECT
# MAGIC     t.*,
# MAGIC     a.* EXCEPT (chassis_no, claim_no)
# MAGIC   FROM smart_claims_dev.03_gold.customer_claim_policy_telematics t
# MAGIC   JOIN smart_claims_dev.03_gold.claim_images_predicted a USING (claim_no)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE smart_claims_dev.02_silver.claims_rules (
# MAGIC   rule_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   rule STRING,
# MAGIC   check_name STRING,
# MAGIC   check_code STRING,
# MAGIC   check_severity STRING,
# MAGIC   is_active BOOLEAN
# MAGIC );

# COMMAND ----------

def insert_rule(rule, name, code, severity, is_active):
    spark.sql(f"""INSERT INTO smart_claims_dev.02_silver.claims_rules
                  (rule, check_name, check_code, check_severity, is_active)
                  VALUES ('{rule}', '{name}', '{code}', '{severity}', {is_active})""")

# COMMAND ----------

valid_policy_date = '''
CASE WHEN to_date(pol_eff_date, "yyyy-MM-dd") <= to_date(claim_date)
      AND to_date(pol_expiry_date, "yyyy-MM-dd") >= to_date(claim_date) THEN "VALID"
     ELSE "NOT VALID"
END
'''
insert_rule('invalid policy date', 'valid_date', valid_policy_date, 'HIGH', True)

exceeds_policy_amount = '''
CASE WHEN CAST(sum_insured AS DOUBLE) >= CAST(total AS DOUBLE) THEN "claim value in the range of premium"
     ELSE "claim value more than premium"
END
'''
insert_rule('exceeds policy amount', 'valid_amount', exceeds_policy_amount, 'HIGH', True)

severity_mismatch = '''
CASE WHEN incident_severity = "Total Loss" AND damage_prediction.label = "major" THEN "Severity matches the report"
     WHEN incident_severity = "Major Damage" AND damage_prediction.label = "minor" THEN "Severity matches the report"
     WHEN (incident_severity = "Minor Damage" OR incident_severity = "Trivial Damage") AND damage_prediction.label = "ok" THEN "Severity matches the report"
     ELSE "Severity does not match"
END
'''
insert_rule('severity mismatch', 'reported_severity_check', severity_mismatch, 'HIGH', True)

exceeds_speed = '''
CASE WHEN telematics_speed <= 45 AND telematics_speed > 0 THEN "Normal Speed"
     WHEN telematics_speed > 45 THEN "High Speed"
     ELSE "Invalid speed"
END
'''
insert_rule('exceeds speed', 'speed_check', exceeds_speed, 'HIGH', True)

release_funds = '''
CASE WHEN reported_severity_check = "Severity matches the report"
      AND valid_amount = "claim value in the range of premium"
      AND valid_date = "VALID" THEN "release funds"
     ELSE "claim needs more investigation"
END
'''
insert_rule('release funds', 'fund_release', release_funds, 'HIGH', True)

# COMMAND ----------

from pyspark.sql.functions import expr

df = spark.table("smart_claims_dev.03_gold.customer_claim_policy_telematics_predicted")
rules = spark.sql("SELECT * FROM smart_claims_dev.02_silver.claims_rules WHERE is_active = true ORDER BY rule_id").collect()

for rule in rules:
    print(rule.rule)
    df = df.withColumn(rule.check_name, expr(rule.check_code))

(df.write.mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable("smart_claims_dev.03_gold.claim_insights"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT claim_no, incident_severity, damage_prediction.label AS photo_damage,
# MAGIC        valid_date, valid_amount, reported_severity_check, speed_check, fund_release
# MAGIC FROM smart_claims_dev.03_gold.claim_insights
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fund_release, COUNT(*) AS claims
# MAGIC FROM smart_claims_dev.03_gold.claim_insights
# MAGIC GROUP BY fund_release