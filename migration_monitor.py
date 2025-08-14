import papermill as pm
import pandas as pd
import os
import traceback
from datetime import datetime
from teradatasql import connect as td_connect
from pyspark.sql import SparkSession
import hashlib

# Spark session for Databricks
spark = SparkSession.builder.getOrCreate()

# Teradata Connection Config
TERADATA_CONFIG = {
    "host": "TERADATA_SERVER",
    "user": "USERNAME",
    "password": "PASSWORD"
}

# Migration notebook configurations
NOTEBOOKS = [
    {"path": "notebooks/migrate_sales.ipynb", "params": {"table": "sales"}, "td_db": "sales_db", "td_table": "sales", "dbx_path": "/mnt/datalake/sales_db/sales"},
    {"path": "notebooks/migrate_customers.ipynb", "params": {"table": "customers"}, "td_db": "cust_db", "td_table": "customers", "dbx_path": "/mnt/datalake/cust_db/customers"}
]

LOG_FILE = "migration_monitoring_log.csv"
os.makedirs("executed_notebooks", exist_ok=True)

def get_teradata_row_count(database, table):
    """Get row count from Teradata."""
    with td_connect(host=TERADATA_CONFIG["host"], user=TERADATA_CONFIG["user"], password=TERADATA_CONFIG["password"]) as conn:
        query = f"SELECT COUNT(*) AS cnt FROM {database}.{table}"
        df = pd.read_sql(query, conn)
        return int(df.iloc[0]["cnt"])

def get_databricks_row_count(delta_path):
    """Get row count from Databricks Delta."""
    df = spark.read.format("delta").load(delta_path)
    return df.count()

def get_data_hash(df, sample_size=10):
    """Create a hash of sampled data for comparison."""
    sample_df = df.limit(sample_size).toPandas()
    hash_val = hashlib.sha256(pd.util.hash_pandas_object(sample_df, index=False).values.tobytes()).hexdigest()
    return hash_val

def run_notebook_with_verification(notebook_config):
    """Run migration notebook and verify results."""
    start_time = datetime.now()
    status = "SUCCESS"
    error_msg = ""
    row_count_match = None
    data_quality_match = None

    output_path = f"executed_notebooks/{os.path.basename(notebook_config['path']).replace('.ipynb', '')}_{start_time.strftime('%Y%m%d_%H%M%S')}.ipynb"

    try:
        # Run notebook
        pm.execute_notebook(
            input_path=notebook_config["path"],
            output_path=output_path,
            parameters=notebook_config["params"],
            log_output=True
        )

        # Row count comparison
        td_count = get_teradata_row_count(notebook_config["td_db"], notebook_config["td_table"])
        dbx_count = get_databricks_row_count(notebook_config["dbx_path"])
        row_count_match = (td_count == dbx_count)

        # Data quality sample comparison
        td_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:teradata://{TERADATA_CONFIG['host']}/Database={notebook_config['td_db']}") \
            .option("dbtable", notebook_config["td_table"]) \
            .option("user", TERADATA_CONFIG["user"]) \
            .option("password", TERADATA_CONFIG["password"]) \
            .load()

        dbx_df = spark.read.format("delta").load(notebook_config["dbx_path"])

        td_hash = get_data_hash(td_df)
        dbx_hash = get_data_hash(dbx_df)
        data_quality_match = (td_hash == dbx_hash)

    except Exception:
        status = "FAILED"
        error_msg = traceback.format_exc()

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    return {
        "notebook": notebook_config["path"],
        "params": str(notebook_config["params"]),
        "start_time": start_time,
        "end_time": end_time,
        "duration_sec": duration,
        "status": status,
        "error": error_msg,
        "output_notebook": output_path,
        "row_count_match": row_count_match,
        "data_quality_match": data_quality_match
    }

if __name__ == "__main__":
    all_runs = []

    for nb_config in NOTEBOOKS:
        print(f"Running {nb_config['path']} with compliance checks...")
        run_data = run_notebook_with_verification(nb_config)
        all_runs.append(run_data)

    # Save logs to CSV
    df = pd.DataFrame(all_runs)
    if os.path.exists(LOG_FILE):
        df_existing = pd.read_csv(LOG_FILE)
        df = pd.concat([df_existing, df], ignore_index=True)
    df.to_csv(LOG_FILE, index=False)

    print("Migration runs completed with compliance checks. Logs saved to", LOG_FILE)
