#!/usr/bin/env python3
"""
CaixaBank CSV -> PostgreSQL migration script.

Files expected in DATA_DIR:
  - users_data.csv
  - cards_data.csv
  - transactions_data.csv
  - mcc_codes.json
  - merchants_dim.csv
  - fraud_labels.csv

Environment variables:
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
  DATA_DIR=/path/to/csv/folder
  SAMPLE_USERS=5   # optional (omit or 0 to load all)
"""

import os
import json
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import psycopg2
import io


PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE", "caixabank")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "")

DATA_DIR = Path(os.getenv("DATA_DIR", "./data")).resolve()
SAMPLE_USERS = int(os.getenv("SAMPLE_USERS", "0")) or None
RANDOM_SEED = int(os.getenv("RANDOM_SEED", "42"))

FILES = {
    "users": DATA_DIR / "users_data.csv",
    "cards": DATA_DIR / "cards_data.csv",
    "tx": DATA_DIR / "transactions_data.csv",
    "mcc": DATA_DIR / "mcc_codes.json",
    "merchants": DATA_DIR / "merchants_dim.csv",
    "fraud": DATA_DIR / "fraud_labels.csv",
}


DDL = """
CREATE TABLE IF NOT EXISTS users (
  user_id BIGINT PRIMARY KEY,
  current_age INTEGER,
  retirement_age INTEGER,
  birth_year INTEGER,
  birth_month INTEGER,
  gender TEXT,
  address TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  per_capita_income NUMERIC,
  yearly_income NUMERIC,
  total_debt NUMERIC,
  credit_score INTEGER,
  num_credit_cards INTEGER
);

CREATE TABLE IF NOT EXISTS cards (
  card_id BIGINT PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(user_id),
  card_brand TEXT,
  card_type TEXT,
  card_number TEXT,
  expires DATE,
  cvv INTEGER,
  has_chip BOOLEAN,
  num_cards_issued INTEGER,
  credit_limit NUMERIC,
  acct_open_date DATE,
  year_pin_last_changed INTEGER,
  card_on_dark_web BOOLEAN
);
CREATE INDEX IF NOT EXISTS idx_cards_user ON cards(user_id);

CREATE TABLE IF NOT EXISTS mcc_categories (
  mcc INTEGER PRIMARY KEY,
  description TEXT
);

CREATE TABLE IF NOT EXISTS merchants (
  merchant_id BIGINT PRIMARY KEY,
  mcc INTEGER REFERENCES mcc_categories(mcc),
  merchant_city TEXT,
  merchant_state TEXT,
  zip TEXT
);

CREATE TABLE IF NOT EXISTS transactions (
  txn_id BIGINT PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(user_id),
  card_id BIGINT NOT NULL REFERENCES cards(card_id),
  merchant_id BIGINT NOT NULL REFERENCES merchants(merchant_id),
  txn_ts TIMESTAMPTZ NOT NULL,
  amount NUMERIC NOT NULL,
  use_chip TEXT,
  merchant_city TEXT,
  merchant_state TEXT,
  zip TEXT,
  mcc INTEGER REFERENCES mcc_categories(mcc),
  errors TEXT
);
CREATE INDEX IF NOT EXISTS idx_txn_user_ts ON transactions(user_id, txn_ts DESC);
CREATE INDEX IF NOT EXISTS idx_txn_merchant_ts ON transactions(merchant_id, txn_ts DESC);

CREATE TABLE IF NOT EXISTS fraud_labels (
  txn_id BIGINT PRIMARY KEY REFERENCES transactions(txn_id) ON DELETE CASCADE,
  is_fraud SMALLINT NOT NULL CHECK (is_fraud IN (0,1))
);
"""


def connect():
    return psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
    )


def table_rowcount(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table};")
    return int(cur.fetchone()[0])


def copy_df(cur, table: str, df: pd.DataFrame, cols: List[str]) -> None:
    """
    Bulk load a DataFrame into Postgres using COPY ... FROM STDIN.
    Uses an in-memory CSV buffer (avoids Windows temp-file locking issues).
    """
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    cur.copy_expert(
        f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH CSV HEADER",
        buf,
    )


def normalize_money(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(s, errors="coerce")


def parse_mm_yyyy_to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.astype(str), format="%m/%Y", errors="coerce").dt.date


def yesno_to_bool(series: pd.Series) -> pd.Series:
    v = series.astype(str).str.strip().str.lower()
    return v.map({"yes": True, "no": False})


def normalize_zip(series: pd.Series) -> pd.Series:
    def clean(v: Any) -> Optional[str]:
        if pd.isna(v):
            return None
        s = str(v).strip()
        if s.endswith(".0"):
            s = s[:-2]
        if not s or s.lower() == "nan":
            return None
        return s

    return series.map(clean)


def load_sources() -> Dict[str, Any]:
    for _, p in FILES.items():
        if not p.exists():
            raise FileNotFoundError(f"Missing dataset file: {p}")

    with open(FILES["mcc"], "r", encoding="utf-8") as f:
        mcc = json.load(f)

    return {
        "users": pd.read_csv(FILES["users"]),
        "cards": pd.read_csv(FILES["cards"]),
        "tx": pd.read_csv(FILES["tx"]),
        "mcc": mcc,
        "merchants": pd.read_csv(FILES["merchants"]),
        "fraud": pd.read_csv(FILES["fraud"]),
    }


def sample_users_from_transactions(tx: pd.DataFrame, n: int) -> List[int]:
    return (
        tx["user_id"]
        .dropna()
        .astype("int64")
        .drop_duplicates()
        .sample(n=n, random_state=RANDOM_SEED)
        .tolist()
    )


def build_tables(src: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    users_raw = src["users"].copy()
    cards_raw = src["cards"].copy()
    tx_raw = src["tx"].copy()
    merchants_raw = src["merchants"].copy()
    fraud_raw = src["fraud"].copy()

    if SAMPLE_USERS:
        ids = sample_users_from_transactions(tx_raw, SAMPLE_USERS)
        tx_raw = tx_raw[tx_raw["user_id"].isin(ids)].copy()
        users_raw = users_raw[users_raw["user_id"].isin(ids)].copy()
        cards_raw = cards_raw[cards_raw["client_id"].isin(ids)].copy()

    users_df = users_raw.copy()
    user_cols = [
        "user_id",
        "current_age",
        "retirement_age",
        "birth_year",
        "birth_month",
        "gender",
        "address",
        "latitude",
        "longitude",
        "per_capita_income",
        "yearly_income",
        "total_debt",
        "credit_score",
        "num_credit_cards",
    ]
    users_df = users_df[[c for c in user_cols if c in users_df.columns]].drop_duplicates(subset=["user_id"])
    for col in ["per_capita_income", "yearly_income", "total_debt"]:
        if col in users_df.columns:
            users_df[col] = normalize_money(users_df[col])

    cards_df = pd.DataFrame(
        {
            "card_id": pd.to_numeric(cards_raw["card_id"], errors="coerce").astype("Int64"),
            "user_id": pd.to_numeric(cards_raw["client_id"], errors="coerce").astype("Int64"),
            "card_brand": cards_raw.get("card_brand"),
            "card_type": cards_raw.get("card_type"),
            "card_number": cards_raw.get("card_number").astype(str),
            "expires": parse_mm_yyyy_to_date(cards_raw.get("expires")),
            "cvv": pd.to_numeric(cards_raw.get("cvv"), errors="coerce").astype("Int64"),
            "has_chip": yesno_to_bool(cards_raw.get("has_chip")),
            "num_cards_issued": pd.to_numeric(cards_raw.get("num_cards_issued"), errors="coerce").astype("Int64"),
            "credit_limit": normalize_money(cards_raw.get("credit_limit")),
            "acct_open_date": parse_mm_yyyy_to_date(cards_raw.get("acct_open_date")),
            "year_pin_last_changed": pd.to_numeric(cards_raw.get("year_pin_last_changed"), errors="coerce").astype("Int64"),
            "card_on_dark_web": yesno_to_bool(cards_raw.get("card_on_dark_web")),
        }
    ).dropna(subset=["card_id", "user_id"])
    cards_df = cards_df.drop_duplicates(subset=["card_id"])

    mcc_df = pd.DataFrame([{"mcc": int(k), "description": v} for k, v in src["mcc"].items()])
    mcc_df["mcc"] = pd.to_numeric(mcc_df["mcc"], errors="coerce").astype("Int64")
    mcc_df = mcc_df.dropna(subset=["mcc"]).drop_duplicates(subset=["mcc"])
    mcc_df = mcc_df.sort_values("mcc").reset_index(drop=True)

    required = ["txn_id", "txn_ts", "user_id", "card_id", "amount", "merchant_id", "mcc"]
    missing = [c for c in required if c not in tx_raw.columns]
    if missing:
        raise ValueError(f"transactions_data.csv missing required columns: {missing}")

    tx_df = pd.DataFrame(
        {
            "txn_id": pd.to_numeric(tx_raw["txn_id"], errors="coerce").astype("Int64"),
            "user_id": pd.to_numeric(tx_raw["user_id"], errors="coerce").astype("Int64"),
            "card_id": pd.to_numeric(tx_raw["card_id"], errors="coerce").astype("Int64"),
            "merchant_id": pd.to_numeric(tx_raw["merchant_id"], errors="coerce").astype("Int64"),
            "txn_ts": pd.to_datetime(tx_raw["txn_ts"], errors="coerce", utc=True),
            "amount": pd.to_numeric(tx_raw["amount"], errors="coerce"),
            "use_chip": tx_raw.get("use_chip"),
            "merchant_city": tx_raw.get("merchant_city"),
            "merchant_state": tx_raw.get("merchant_state"),
            "zip": normalize_zip(tx_raw.get("zip")),
            "mcc": pd.to_numeric(tx_raw["mcc"], errors="coerce").astype("Int64"),
            "errors": tx_raw.get("errors"),
        }
    ).dropna(subset=["txn_id", "user_id", "card_id", "merchant_id", "txn_ts", "amount", "mcc"])
    tx_df = tx_df.drop_duplicates(subset=["txn_id"])

    mcc_df = mcc_df[mcc_df["mcc"].isin(tx_df["mcc"].unique())].copy()

    merchants_df = merchants_raw.copy()
    merchants_df["merchant_id"] = pd.to_numeric(merchants_df["merchant_id"], errors="coerce").astype("Int64")
    merchants_df["mcc"] = pd.to_numeric(merchants_df["mcc"], errors="coerce").astype("Int64")
    merchants_df["zip"] = normalize_zip(merchants_df["zip"])
    merchants_df = (
        merchants_df[["merchant_id", "mcc", "merchant_city", "merchant_state", "zip"]]
        .dropna(subset=["merchant_id"])
        .drop_duplicates(subset=["merchant_id"])
        .sort_values("merchant_id")
        .reset_index(drop=True)
    )
    merchants_df = merchants_df[merchants_df["merchant_id"].isin(tx_df["merchant_id"].unique())].copy()

    fraud_df = fraud_raw.copy()
    req_fraud = ["txn_id", "is_fraud"]
    missing_fraud = [c for c in req_fraud if c not in fraud_df.columns]
    if missing_fraud:
        raise ValueError(f"fraud_labels.csv missing required columns: {missing_fraud}")
    fraud_df["txn_id"] = pd.to_numeric(fraud_df["txn_id"], errors="coerce").astype("Int64")
    fraud_df["is_fraud"] = pd.to_numeric(fraud_df["is_fraud"], errors="coerce").fillna(0).astype("Int64")
    fraud_df = fraud_df[fraud_df["txn_id"].isin(tx_df["txn_id"].unique())]
    fraud_df = fraud_df.dropna(subset=["txn_id"]).drop_duplicates(subset=["txn_id"])
    fraud_df = fraud_df.sort_values("txn_id").reset_index(drop=True)

    return {
        "users": users_df,
        "cards": cards_df,
        "mcc_categories": mcc_df,
        "merchants": merchants_df,
        "transactions": tx_df,
        "fraud_labels": fraud_df,
    }


def migrate_if_empty(conn, table: str, df: pd.DataFrame, cols: List[str]) -> None:
    with conn.cursor() as cur:
        n = table_rowcount(cur, table)
        if n > 0:
            print(f"SKIP {table}: already has {n} rows.")
            return
        print(f"LOAD {table}: inserting {len(df)} rows...")
        copy_df(cur, table, df[cols], cols)
    conn.commit()


def main():
    print(f"DATA_DIR: {DATA_DIR}")
    print(f"SAMPLE_USERS: {SAMPLE_USERS if SAMPLE_USERS else 'ALL'}")
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

        src = load_sources()
        tables = build_tables(src)

        migrate_if_empty(conn, "users", tables["users"], list(tables["users"].columns))
        migrate_if_empty(
            conn,
            "cards",
            tables["cards"],
            [
                "card_id",
                "user_id",
                "card_brand",
                "card_type",
                "card_number",
                "expires",
                "cvv",
                "has_chip",
                "num_cards_issued",
                "credit_limit",
                "acct_open_date",
                "year_pin_last_changed",
                "card_on_dark_web",
            ],
        )
        migrate_if_empty(conn, "mcc_categories", tables["mcc_categories"], ["mcc", "description"])
        migrate_if_empty(
            conn,
            "merchants",
            tables["merchants"],
            ["merchant_id", "mcc", "merchant_city", "merchant_state", "zip"],
        )
        migrate_if_empty(
            conn,
            "transactions",
            tables["transactions"],
            [
                "txn_id",
                "user_id",
                "card_id",
                "merchant_id",
                "txn_ts",
                "amount",
                "use_chip",
                "merchant_city",
                "merchant_state",
                "zip",
                "mcc",
                "errors",
            ],
        )
        migrate_if_empty(conn, "fraud_labels", tables["fraud_labels"], ["txn_id", "is_fraud"])
        print("Done")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
