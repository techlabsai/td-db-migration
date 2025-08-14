# Teradata to Databricks migration

## About
Compliance-grade wrapper to verify row counts and data quality of migration between Teradata and Databricks.
Each migration is provided as Jupyter notebooks to the wrapper.
The wrapper kicks off the execution of notebooks and after each migration notebook finished captures compliance and monitoring data.

## Functions
After each notebook run:
- Row Count Check
  - Query Teradata table row count
  - Query Databricks Delta table row count
  - Compare (within tolerance for rounding/dates if needed)
- Data Quality Sample Check
  - Sample N rows from Teradata & Databricks
  - Compare hashes or specific key fields to ensure values match
- Detailed Compliance Log
  - Migration status
  - Row count match result
  - Data quality match result
  - Notes if mismatches detected

## How It Works
- Runs your migration notebook with papermill
- Captures execution metadata: start/end time, duration, error trace
- Verifies migration correctness:
 - Compares row counts
 - Compares sample data hash
 - Stores results in migration_monitoring_log.csv for compliance audit

## Compliance data captured
- Audit trail: Which notebook ran, when, with what parameters
- Evidence of data correctness (row count + sample match)
- Failures are logged with full trace for investigation
- Keeps executed copies of notebooks for review
  
## Integration options
Can be integrated with:
- Databricks Jobs Scheduler
- Azure Data Factory
- Airflow

## Extensions
Alerting extensions can be found in the ```extensions.py```, which provides below functions
 - Migration fails - Immediate Slack & Email ping
 - Row counts differ - Alert
 - Sample data hash differs -  Alert
 - Logs still written to migration_monitoring_log.csv for full audit

### Deployment
 - Put this wrapper on a Databricks Job or cron
 - Make sure Slack Webhook URL & SMTP details are stored in secure configs (not in code)
 - Optionally integrate with Azure Key Vault or Databricks Secrets for credentials



