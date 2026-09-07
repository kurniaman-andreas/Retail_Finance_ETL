import os

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# config

DATA_FILE = Path(
    os.getenv(
        "SOURCE_FILE",
        "/opt/airflow/data/Data Engineer Analyst - Study Case.xlsx"
    )
)

SQL_DIR = Path(
    os.getenv(
        "SQL_DIR",
        "/opt/airflow/sql"
    )
)

def execute_sql_file(connection, relative_path: str, params: dict):
    sql_path = SQL_DIR / relative_path
    if not sql_path.exists():
        raise AirflowFailException(
            f"SQL file does not exist: {sql_path}"
        )
    with open(sql_path, "r", encoding="utf-8") as f:
        sql_content = f.read()
    connection.exec_driver_sql(sql_content, params)


# connection

def get_connection():
    from sqlalchemy import create_engine
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        uri = hook.get_uri()
        if uri.startswith("postgres://"):
            uri = uri.replace("postgres://", "postgresql://", 1)
        if "postgresql+psycopg2://" not in uri and uri.startswith("postgresql://"):
            uri = uri.replace("postgresql://", "postgresql+psycopg2://", 1)
        return create_engine(uri)
    except Exception:
        db_user = os.getenv("DB_USER", "retail_user")
        db_pass = os.getenv("DB_PASSWORD", "retail_password")
        db_host = os.getenv("DB_HOST", "postgres")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "retail_finance")
        conn_url = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        return create_engine(conn_url)



@dag(
    dag_id="retail_financing_pipeline",

    start_date=datetime(2025, 1, 2),

    schedule="@daily",

    catchup=True,

    max_active_runs=1,

    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },

    tags=["retail-financing", "etl"],
)
def retail_financing_pipeline():


    start = EmptyOperator(
        task_id="start"
    )

    # ingest transactions

    @task(
        task_id="ingest_transactions",
        execution_timeout=timedelta(hours=2)
    )
    def ingest_transactions(data_interval_start=None):

        if not DATA_FILE.exists():
            raise AirflowFailException(
                f"Data file does not exist: {DATA_FILE}"
            )

        try:
            df = pd.read_excel(
                DATA_FILE,
                sheet_name="transactions"
            )

        except Exception as exc:
            raise AirflowFailException(
                f"Failed to read transactions sheet: {exc}"
            ) from exc

        if df.empty:
            raise AirflowFailException(
                "Transactions sheet is empty"
            )

        required_columns = [
            "transaction_id",
            "customer_id",
            "branch_id",
            "transaction_date",
            "amount",
            "payment_method",
            "transaction_status",
            "channel",
            "updated_at",
            "merchant_category",
            "device_type",
            "currency",
            "fraud_flag",
            "promo_code",
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing_columns:
            raise AirflowFailException(
                "Missing columns in transactions sheet: "
                f"{missing_columns}"
            )

        # Business date = H-1
        biz_date = data_interval_start.subtract(days=1)

        print(
            f"Processing transactions for biz_date="
            f"{biz_date.strftime('%Y-%m-%d')}"
        )

       

        df["updated_at"] = pd.to_datetime(
            df["updated_at"],
            errors="coerce"
        )

        # Only take records updated on H-1
        df = df[
            df["updated_at"].dt.date == biz_date.date()
        ].copy()


        if df.empty:
            print(
                f"No transactions found for "
                f"biz_date={biz_date.strftime('%Y-%m-%d')}"
            )
            return 0

        

        df["batch_date"] = data_interval_start.date()

        # load to bronze

        try:

            engine = get_connection()

            df.to_sql(
                name="bronze_transactions",
                con=engine,
                if_exists="append",
                index=False,
                method="multi"
            )

        except Exception as exc:

            raise AirflowFailException(
                f"Failed to load bronze_transactions: {exc}"
            ) from exc

        print(
            f"Inserted {len(df)} rows into "
            f"bronze_transactions"
        )

        return len(df)

    # ingest customers

    @task(
        task_id="ingest_customers",
        execution_timeout=timedelta(hours=2)
    )
    def ingest_customers(data_interval_start=None):

        if not DATA_FILE.exists():
            raise AirflowFailException(
                f"Data file does not exist: {DATA_FILE}"
            )

        # read excel sheet

        try:
            df = pd.read_excel(
                DATA_FILE,
                sheet_name="customers"
            )

        except Exception as exc:
            raise AirflowFailException(
                f"Failed to read customers sheet: {exc}"
            ) from exc


        if df.empty:
            raise AirflowFailException(
                "Customers sheet is empty"
            )

        required_columns = [
            "customer_id",
            "customer_name",
            "city",
            "registration_date",
            "customer_status",
            "customer_segment",
            "email",
            "phone_number",
            "birth_date",
            "occupation",
            "income_band",
            "kyc_status",
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing_columns:
            raise AirflowFailException(
                "Missing columns in customers sheet: "
                f"{missing_columns}"
            )

        

        df["batch_date"] = data_interval_start.date()

        # load to bronze

        try:

            engine = get_connection()

            df.to_sql(
                name="bronze_customers",
                con=engine,
                if_exists="append",
                index=False,
                method="multi"
            )

        except Exception as exc:

            raise AirflowFailException(
                f"Failed to load bronze_customers: {exc}"
            ) from exc

        print(
            f"Inserted {len(df)} rows into "
            f"bronze_customers"
        )

        return len(df)

    # ingest branches

    @task(
        task_id="ingest_branches",
        execution_timeout=timedelta(hours=2)
    )
    def ingest_branches(data_interval_start=None):

        if not DATA_FILE.exists():
            raise AirflowFailException(
                f"Data file does not exist: {DATA_FILE}"
            )

        try:
            df = pd.read_excel(
                DATA_FILE,
                sheet_name="branches"
            )

        except Exception as exc:
            raise AirflowFailException(
                f"Failed to read branches sheet: {exc}"
            ) from exc


        if df.empty:
            raise AirflowFailException(
                "Branches sheet is empty"
            )

        required_columns = [
            "branch_id",
            "branch_name",
            "region",
            "branch_status",
            "branch_type",
            "opening_date",
            "manager_name",
            "city",
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing_columns:
            raise AirflowFailException(
                "Missing columns in branches sheet: "
                f"{missing_columns}"
            )

        df["batch_date"] = data_interval_start.date()

        # load to bronze

        try:

            engine = get_connection()

            df.to_sql(
                name="bronze_branches",
                con=engine,
                if_exists="append",
                index=False,
                method="multi"
            )

        except Exception as exc:

            raise AirflowFailException(
                f"Failed to load bronze_branches: {exc}"
            ) from exc

        print(
            f"Inserted {len(df)} rows into "
            f"bronze_branches"
        )

        return len(df)

    # task bronze_ready

    bronze_ready = EmptyOperator(
        task_id="bronze_ready"
    )

    # transform staging

    @task(
        task_id="transform_staging",
        execution_timeout=timedelta(hours=2)
    )
    def transform_staging(data_interval_start=None):

        batch_date = data_interval_start.date()

        engine = get_connection()

        try:

            with engine.begin() as connection:

                execute_sql_file(
                    connection,
                    "staging/stg_transactions.sql",
                    {"batch_date": batch_date}
                )

                execute_sql_file(
                    connection,
                    "staging/stg_customers.sql",
                    {"batch_date": batch_date}
                )

                execute_sql_file(
                    connection,
                    "staging/stg_branches.sql",
                    {"batch_date": batch_date}
                )

        except Exception as exc:

            raise AirflowFailException(
                f"Staging transformation failed: {exc}"
            ) from exc

        print(
            f"Staging transformation completed for "
            f"batch_date={batch_date}"
        )

    # dq validation

    @task(
        task_id="dq_validation",
        execution_timeout=timedelta(hours=2)
    )
    def dq_validation(data_interval_start=None):

        batch_date = data_interval_start.date()

        engine = get_connection()

        try:
            with engine.connect() as connection:
                sql_path = SQL_DIR / "dq/validate.sql"
                if not sql_path.exists():
                    raise AirflowFailException(
                        f"SQL file does not exist: {sql_path}"
                    )
                with open(sql_path, "r", encoding="utf-8") as f:
                    sql_content = f.read()

                rows = connection.exec_driver_sql(
                    sql_content,
                    {"batch_date": batch_date}
                ).mappings().all()

        except Exception as exc:
            raise AirflowFailException(
                f"DQ validation query execution failed: {exc}"
            ) from exc

        formatted_date = batch_date.strftime("%Y-%m-%d")
        lines = []
        lines.append("==================================================")
        lines.append("DATA QUALITY VALIDATION")
        lines.append(f"batch_date = {formatted_date}")
        lines.append("==================================================")
        lines.append("")
        lines.append(f"{'rule_name':<26} {'failed_count':<12}")
        lines.append("----------------------------------------")

        total_failed = 0
        for row in rows:
            rule_name = str(row["rule_name"])
            failed_count = int(row["failed_count"])
            total_failed += failed_count
            lines.append(f"{rule_name:<26} {failed_count:<12}")

        lines.append("")
        if total_failed == 0:
            lines.append("DQ STATUS: PASSED")
        else:
            lines.append("DQ STATUS: FAILED")
        lines.append("==================================================")

        log_output = "\n".join(lines)
        print(log_output)

        if total_failed > 0:
            raise AirflowFailException(
                f"Data quality validation FAILED for batch_date={formatted_date} with {total_failed} total issues."
            )

    # load dwh

    @task(
        task_id="load_dwh",
        execution_timeout=timedelta(hours=2)
    )
    def load_dwh(data_interval_start=None):

        batch_date = data_interval_start.date()

        engine = get_connection()

        try:

            with engine.begin() as connection:

                execute_sql_file(
                    connection,
                    "dwh/load_customers.sql",
                    {"batch_date": batch_date}
                )

                execute_sql_file(
                    connection,
                    "dwh/load_branches.sql",
                    {"batch_date": batch_date}
                )

                execute_sql_file(
                    connection,
                    "dwh/load_transactions.sql",
                    {"batch_date": batch_date}
                )

        except Exception as exc:

            raise AirflowFailException(
                f"DWH load failed: {exc}"
            ) from exc

        print(
            f"DWH load completed for "
            f"batch_date={batch_date}"
        )

    # end operator

    end = EmptyOperator(
        task_id="end"
    )

    transactions = ingest_transactions()
    customers = ingest_customers()
    branches = ingest_branches()

    staging = transform_staging()

    dq = dq_validation()

    dwh = load_dwh()

   

    start >> [
        transactions,
        customers,
        branches
    ] >> bronze_ready

    bronze_ready >> staging >> dq >> dwh >> end


dag = retail_financing_pipeline()

