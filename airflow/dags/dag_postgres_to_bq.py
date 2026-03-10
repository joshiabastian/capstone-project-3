from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd
import os

# Config
POSTGRES_CONN_ID = "postgres_library"
GCP_PROJECT_ID = "jcdeah-008"
BQ_DATASET = "yosia_perpustakaan_capstone3"
GCP_CREDENTIALS = "/opt/airflow/gcp/service-account.json"

TABLES = ["members", "books", "loans", "fines"]

# ─── Default Args ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "capstone3",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 3, 6),
}

# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════


def get_bq_client() -> bigquery.Client:
    """Buat BigQuery client dari service account."""
    return bigquery.Client.from_service_account_json(
        GCP_CREDENTIALS,
        project=GCP_PROJECT_ID,
    )


def ensure_table_exists(
    client: bigquery.Client, table_id: str, schema: list, partition_field: str
):
    """Buat tabel BigQuery jika belum ada, dengan partisi dan schema incremental."""
    try:
        client.get_table(table_id)
        print(f"✅ Tabel {table_id} sudah ada")
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        # Partition by created_at (DAY)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
        client.create_table(table)
        print(f"✅ Tabel {table_id} berhasil dibuat")


# ─── BigQuery Schema per tabel ────────────────────────────────────────────────
BQ_SCHEMAS = {
    "members": [
        bigquery.SchemaField("member_id", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("phone", "STRING"),
        bigquery.SchemaField("membership_status", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ],
    "books": [
        bigquery.SchemaField("book_id", "INTEGER"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("author", "STRING"),
        bigquery.SchemaField("genre", "STRING"),
        bigquery.SchemaField("stock", "INTEGER"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ],
    "loans": [
        bigquery.SchemaField("loan_id", "INTEGER"),
        bigquery.SchemaField("member_id", "INTEGER"),
        bigquery.SchemaField("book_id", "INTEGER"),
        bigquery.SchemaField("loan_date", "DATE"),
        bigquery.SchemaField("due_date", "DATE"),
        bigquery.SchemaField("return_date", "DATE"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ],
    "fines": [
        bigquery.SchemaField("fine_id", "INTEGER"),
        bigquery.SchemaField("loan_id", "INTEGER"),
        bigquery.SchemaField("member_id", "INTEGER"),
        bigquery.SchemaField("fine_amount", "FLOAT64"),
        bigquery.SchemaField("is_paid", "BOOLEAN"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ],
}


# ══════════════════════════════════════════════════════════════════════════════
# OPERATOR FUNCTION — Extract semua tabel dari Postgres (1 fungsi, 1 task)
# ══════════════════════════════════════════════════════════════════════════════


def extract_all_tables(**context):
    """
    Extract semua tabel dari Postgres dengan filter H-1 (created_at).
    Hasil disimpan ke XCom per tabel.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    exec_date = context["execution_date"]
    # Filter H-1: data yang created_at = tanggal kemarin
    filter_date = (exec_date - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"📅 Extracting data untuk tanggal: {filter_date}")

    extracted = {}
    for table in TABLES:
        query = f"""
            SELECT *
            FROM {table}
            WHERE DATE(created_at) = '{filter_date}'
        """
        df = hook.get_pandas_df(query)
        extracted[table] = df.to_json(date_format="iso", default_handler=str)
        print(f"✅ Extracted {len(df)} rows dari {table}")

    # Push ke XCom supaya bisa dipakai task berikutnya
    context["ti"].xcom_push(key="extracted_data", value=extracted)


# ══════════════════════════════════════════════════════════════════════════════
# LOAD FUNCTIONS — Load setiap tabel ke BigQuery (task terpisah per tabel)
# ══════════════════════════════════════════════════════════════════════════════


def load_table_to_bq(table_name: str, **context):
    """
    Load 1 tabel dari XCom ke BigQuery.
    Schema incremental: WRITE_APPEND (tambah data, tidak replace).
    Schema partition: partisi by created_at per hari.
    """
    # Ambil data dari XCom
    extracted = context["ti"].xcom_pull(
        key="extracted_data", task_ids="extract_all_tables"
    )

    if not extracted or table_name not in extracted:
        print(f"⚠️  Tidak ada data untuk {table_name}, skip")
        return

    df = pd.read_json(extracted[table_name])

    if df.empty:
        print(f"Data {table_name} kosong untuk H-1, skip")
        return

    # Fix tipe data per tabel
    date_cols = {"loans": ["loan_date", "due_date", "return_date"]}
    int_cols = {
        "loans": ["loan_id", "member_id", "book_id"],
        "fines": ["fine_id", "loan_id", "member_id"],
    }
    bool_cols = {"fines": ["is_paid"]}
    numeric_cols = {"fines": ["fine_amount"]}

    if table_name in date_cols:
        for col in date_cols[table_name]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    if table_name in int_cols:
        for col in int_cols[table_name]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if table_name in bool_cols:
        for col in bool_cols[table_name]:
            if col in df.columns:
                df[col] = df[col].astype(bool)

    if table_name in numeric_cols:
        for col in numeric_cols[table_name]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    print(f"📤 Loading {len(df)} rows dari {table_name} ke BigQuery...")

    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"

    # Pastikan tabel ada (dengan partisi)
    ensure_table_exists(
        client=client,
        table_id=table_id,
        schema=BQ_SCHEMAS[table_name],
        partition_field="created_at",
    )

    # Load config: WRITE_APPEND = incremental (tidak replace data lama)
    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMAS[table_name],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    # Load dataframe ke BigQuery
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # tunggu sampai selesai

    print(f"✅ Berhasil load {len(df)} rows ke {table_id}")


# ══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="dag_postgres_to_bq",
    description="Ingest data dari PostgreSQL ke BigQuery setiap hari (H-1)",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["capstone3", "bigquery", "ingest"],
) as dag:

    # ─── Task 1: Extract semua tabel (1 task) ─────────────────────────────────
    t_extract = PythonOperator(
        task_id="extract_all_tables",
        python_callable=extract_all_tables,
        provide_context=True,
    )

    # ─── Task 2: Load per tabel ke BigQuery (task terpisah per tabel) ─────────
    load_tasks = []
    for table in TABLES:
        t_load = PythonOperator(
            task_id=f"load_{table}_to_bq",
            python_callable=load_table_to_bq,
            op_kwargs={"table_name": table},
            provide_context=True,
        )
        load_tasks.append(t_load)

    # ─── Task Dependencies ────────────────────────────────────────────────────
    # Extract dulu, baru semua load task jalan paralel
    t_extract >> load_tasks
