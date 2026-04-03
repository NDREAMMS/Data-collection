# 0.6_load.py
# Role : charger data/processed/transformed_elysee.csv dans PostgreSQL (Data Warehouse)
#
# Prerequis:
#   - pip install psycopg2-binary python-dotenv pandas
#   - un serveur PostgreSQL accessible
#   - un fichier .env a la racine (voir .env.example)
#
# Usage:
#   myenv\Scripts\python scripts\0.6_load.py --if-exists replace

import os
import sys
import argparse
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def pg_type_for_dtype(dtype) -> str:
    # Mapping simple des dtypes pandas -> PostgreSQL
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    return "TEXT"


def build_create_table_sql(df: pd.DataFrame, schema: str, table: str) -> str:
    cols = []
    for col in df.columns:
        cols.append(f"{quote_ident(col)} {pg_type_for_dtype(df[col].dtype)}")
    cols_sql = ",\n  ".join(cols)
    return f"CREATE TABLE {quote_ident(schema)}.{quote_ident(table)} (\n  {cols_sql}\n);"


def main() -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Load transformed_elysee.csv into PostgreSQL.")
    parser.add_argument("--csv", default="data/processed/transformed_elysee.csv", help="Path to CSV file")
    parser.add_argument("--schema", default=os.getenv("DB_SCHEMA", "public"), help="PostgreSQL schema")
    parser.add_argument("--table", default=os.getenv("DB_TABLE", "airbnb_elysee_transformed"), help="Destination table")
    parser.add_argument(
        "--if-exists",
        default=os.getenv("IF_EXISTS", "fail"),
        choices=["fail", "replace", "append"],
        help="What to do if table exists",
    )

    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"[ERREUR] CSV introuvable: {csv_path}")
        return 1

    db_user = os.getenv("DB_USER", "")
    db_password = os.getenv("DB_PASSWORD", "")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "")

    missing = [k for k, v in {"DB_USER": db_user, "DB_PASSWORD": db_password, "DB_NAME": db_name}.items() if not v]
    if missing:
        print(f"[ERREUR] Variables manquantes dans .env: {', '.join(missing)}")
        return 1

    try:
        import psycopg2
    except ModuleNotFoundError:
        print("[ERREUR] Module manquant: psycopg2")
        print("Installez: pip install psycopg2-binary")
        return 1

    # Lire le CSV pour inferer les types et obtenir la liste des colonnes
    df = pd.read_csv(csv_path, low_memory=False)
    if df.empty:
        print("[ERREUR] CSV vide, rien a charger.")
        return 1

    conn = psycopg2.connect(
        host=db_host,
        port=int(db_port),
        dbname=db_name,
        user=db_user,
        password=db_password,
    )
    conn.autocommit = False

    schema = args.schema
    table = args.table
    full_table = f"{quote_ident(schema)}.{quote_ident(table)}"

    with conn:
        with conn.cursor() as cur:
            # Schema
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)};")

            # Existence table
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                );
                """,
                (schema, table),
            )
            exists = bool(cur.fetchone()[0])

            if exists and args.if_exists == "fail":
                print(f"[ERREUR] Table deja existante: {schema}.{table} (utilisez --if-exists replace|append)")
                return 1

            if exists and args.if_exists == "replace":
                cur.execute(f"DROP TABLE {full_table};")
                exists = False

            if not exists:
                create_sql = build_create_table_sql(df, schema, table)
                cur.execute(create_sql)

            # Chargement via COPY
            columns_sql = ", ".join(quote_ident(c) for c in df.columns)
            copy_sql = (
                f"COPY {full_table} ({columns_sql}) "
                "FROM STDIN WITH (FORMAT csv, HEADER true, NULL '', ENCODING 'UTF8')"
            )
            with csv_path.open("r", encoding="utf-8", newline="") as f:
                cur.copy_expert(copy_sql, f)

            # Index de base (si colonne id presente)
            if "id" in df.columns:
                idx_name = f"idx_{table}_id"
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS {quote_ident(idx_name)} ON {full_table} ({quote_ident('id')});"
                )

    conn.close()
    print(f"[OK] Charge {len(df):,} lignes -> {schema}.{table}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

