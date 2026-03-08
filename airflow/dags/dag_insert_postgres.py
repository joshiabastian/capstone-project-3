from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import random
from faker import Faker

# Config
fake = Faker("id_ID")
CONN_ID = "postgres_library"
ROWS = 5

# Default Args
default_args = {
    "owner": "capstone3",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 3, 6),
}

# HELPER FUNCTIONS


def get_existing_ids(hook, table: str, id_col: str) -> list:
    """Ambil semua ID yang sudah ada di tabel."""
    rows = hook.get_records(f"SELECT {id_col} FROM {table}")
    return [r[0] for r in rows] if rows else []


# TASK FUNCTIONS


def insert_members():
    """Insert 5 random members ke tabel members."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    statuses = ["active", "active", "active", "inactive"]  # 75% active

    records = []
    for _ in range(ROWS):
        records.append(
            (
                fake.name(),
                f"{fake.first_name().lower()}{random.randint(1,999)}@{random.choice(['gmail.com','yahoo.com'])}",
                fake.phone_number()[:20],
                random.choice(statuses),
                datetime.now(),
            )
        )

    hook.insert_rows(
        table="members",
        rows=records,
        target_fields=["name", "email", "phone", "membership_status", "created_at"],
    )
    print(f"✅ Inserted {ROWS} rows into members")


def insert_books():
    """Insert 5 random books ke tabel books."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    genres = [
        "Novel",
        "Fiksi",
        "Non-Fiksi",
        "Self-Help",
        "Sejarah",
        "Biografi",
        "Sains",
        "Filsafat",
        "Romansa",
        "Petualangan",
    ]

    records = []
    for _ in range(ROWS):
        records.append(
            (
                fake.catch_phrase()[:200],  # judul buku random
                fake.name(),  # nama penulis
                random.choice(genres),
                random.randint(1, 10),  # stok
                datetime.now(),
            )
        )

    hook.insert_rows(
        table="books",
        rows=records,
        target_fields=["title", "author", "genre", "stock", "created_at"],
    )
    print(f"✅ Inserted {ROWS} rows into books")


def insert_loans():
    """Insert 5 random loans ke tabel loans.
    FK: member_id → members, book_id → books
    """
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    member_ids = get_existing_ids(hook, "members", "member_id")
    book_ids = get_existing_ids(hook, "books", "book_id")

    if not member_ids or not book_ids:
        print("⚠️  Tidak ada member/book, skip insert loans")
        return

    statuses = ["borrowed", "returned", "overdue"]

    records = []
    for _ in range(ROWS):
        loan_date = datetime.now().date() - timedelta(days=random.randint(0, 30))
        due_date = loan_date + timedelta(days=14)
        status = random.choice(statuses)
        return_date = (
            loan_date + timedelta(days=random.randint(1, 20))
            if status == "returned"
            else None
        )

        records.append(
            (
                random.choice(member_ids),
                random.choice(book_ids),
                loan_date,
                due_date,
                return_date,
                status,
                datetime.now(),
            )
        )

    hook.insert_rows(
        table="loans",
        rows=records,
        target_fields=[
            "member_id",
            "book_id",
            "loan_date",
            "due_date",
            "return_date",
            "status",
            "created_at",
        ],
    )
    print(f"✅ Inserted {ROWS} rows into loans")


def insert_fines():
    """Insert 5 random fines ke tabel fines.
    FK: loan_id → loans, member_id → members
    """
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    loan_ids = get_existing_ids(hook, "loans", "loan_id")
    member_ids = get_existing_ids(hook, "members", "member_id")

    if not loan_ids or not member_ids:
        print("⚠️  Tidak ada loan/member, skip insert fines")
        return

    records = []
    for _ in range(ROWS):
        days_late = random.randint(1, 30)
        fine_amount = round(days_late * 1000, 2)  # Rp 1.000 per hari

        records.append(
            (
                random.choice(loan_ids),
                random.choice(member_ids),
                fine_amount,
                random.choice([True, False]),
                datetime.now(),
            )
        )

    hook.insert_rows(
        table="fines",
        rows=records,
        target_fields=["loan_id", "member_id", "fine_amount", "is_paid", "created_at"],
    )
    print(f"✅ Inserted {ROWS} rows into fines")


# DAG DEFINITION

with DAG(
    dag_id="dag_insert_postgres",
    description="Insert random data ke PostgreSQL setiap 1 jam",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
    tags=["capstone3", "postgres", "insert"],
) as dag:

    t_members = PythonOperator(
        task_id="insert_members",
        python_callable=insert_members,
    )

    t_books = PythonOperator(
        task_id="insert_books",
        python_callable=insert_books,
    )

    t_loans = PythonOperator(
        task_id="insert_loans",
        python_callable=insert_loans,
    )

    t_fines = PythonOperator(
        task_id="insert_fines",
        python_callable=insert_fines,
    )

    # Task Dependencies
    # members dan books harus ada dulu sebelum loans
    # loans harus ada dulu sebelum fines
    [t_members, t_books] >> t_loans >> t_fines
