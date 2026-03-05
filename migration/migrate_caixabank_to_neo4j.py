#!/usr/bin/env python3
"""
CaixaBank files -> Neo4j migration script.

Files expected in DATA_DIR:
  - users_data.csv
  - cards_data.csv
  - transactions_data.csv
  - mcc_codes.json (preferred) OR categories_dim.csv (fallback)
  - merchants_dim.csv
  - fraud_labels.csv

Environment variables:
  NEO4J_URI=bolt://localhost:7687
  NEO4J_USER=neo4j
  NEO4J_PASSWORD=...
  NEO4J_DATABASE=neo4j
  DATA_DIR=./data
  SAMPLE_USERS=0
  RANDOM_SEED=42
  BATCH_SIZE=5000
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError


NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j12345")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

DATA_DIR = Path(os.getenv("DATA_DIR", "./data")).resolve()
SAMPLE_USERS = int(os.getenv("SAMPLE_USERS", "0")) or None
RANDOM_SEED = int(os.getenv("RANDOM_SEED", "42"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))

FILES = {
    "users": DATA_DIR / "users_data.csv",
    "cards": DATA_DIR / "cards_data.csv",
    "tx": DATA_DIR / "transactions_data.csv",
    "mcc": DATA_DIR / "mcc_codes.json",
    "categories_csv": DATA_DIR / "categories_dim.csv",
    "merchants": DATA_DIR / "merchants_dim.csv",
    "fraud": DATA_DIR / "fraud_labels.csv",
}


def normalize_money(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(s, errors="coerce")


def parse_mm_yyyy_to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.astype(str), format="%m/%Y", errors="coerce").dt.strftime("%Y-%m-%d")


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


def ensure_files() -> None:
    required = ["users", "cards", "tx", "merchants", "fraud"]
    for key in required:
        if not FILES[key].exists():
            raise FileNotFoundError(f"Missing required file for {key}: {FILES[key]}")

    if not FILES["mcc"].exists() and not FILES["categories_csv"].exists():
        raise FileNotFoundError(
            "Missing category source: expected mcc_codes.json or categories_dim.csv in DATA_DIR"
        )


def load_sources() -> Dict[str, Any]:
    ensure_files()

    if FILES["mcc"].exists():
        with open(FILES["mcc"], "r", encoding="utf-8") as f:
            mcc = json.load(f)
    else:
        categories = pd.read_csv(FILES["categories_csv"])
        if "mcc" not in categories.columns or "description" not in categories.columns:
            raise ValueError("categories_dim.csv must contain 'mcc' and 'description' columns")
        mcc = {
            str(int(v["mcc"])): v["description"]
            for _, v in categories.dropna(subset=["mcc", "description"]).iterrows()
        }

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
    users_df["user_id"] = pd.to_numeric(users_df["user_id"], errors="coerce").astype("Int64")
    for col in ["per_capita_income", "yearly_income", "total_debt"]:
        if col in users_df.columns:
            users_df[col] = normalize_money(users_df[col])
    users_df = users_df.dropna(subset=["user_id"]).drop_duplicates(subset=["user_id"])

    cards_df = cards_raw.copy()
    cards_df["card_id"] = pd.to_numeric(cards_df["card_id"], errors="coerce").astype("Int64")
    cards_df["user_id"] = pd.to_numeric(cards_df["client_id"], errors="coerce").astype("Int64")
    cards_df["expires"] = parse_mm_yyyy_to_date(cards_df["expires"])
    cards_df["acct_open_date"] = parse_mm_yyyy_to_date(cards_df["acct_open_date"])
    cards_df["has_chip"] = yesno_to_bool(cards_df["has_chip"])
    cards_df["card_on_dark_web"] = yesno_to_bool(cards_df["card_on_dark_web"])
    cards_df["credit_limit"] = normalize_money(cards_df["credit_limit"])
    cards_df = cards_df.dropna(subset=["card_id", "user_id"]).drop_duplicates(subset=["card_id"])
    cards_df = cards_df.drop(columns=["client_id"], errors="ignore")

    mcc_df = pd.DataFrame([{"mcc": int(k), "description": v} for k, v in src["mcc"].items()])
    mcc_df["mcc"] = pd.to_numeric(mcc_df["mcc"], errors="coerce").astype("Int64")
    mcc_df = mcc_df.dropna(subset=["mcc"]).drop_duplicates(subset=["mcc"])

    required_tx = ["txn_id", "txn_ts", "user_id", "card_id", "amount", "merchant_id", "mcc"]
    missing_tx = [c for c in required_tx if c not in tx_raw.columns]
    if missing_tx:
        raise ValueError(f"transactions_data.csv missing required columns: {missing_tx}")

    tx_df = tx_raw.copy()
    tx_df["txn_id"] = pd.to_numeric(tx_df["txn_id"], errors="coerce").astype("Int64")
    tx_df["user_id"] = pd.to_numeric(tx_df["user_id"], errors="coerce").astype("Int64")
    tx_df["card_id"] = pd.to_numeric(tx_df["card_id"], errors="coerce").astype("Int64")
    tx_df["merchant_id"] = pd.to_numeric(tx_df["merchant_id"], errors="coerce").astype("Int64")
    tx_df["mcc"] = pd.to_numeric(tx_df["mcc"], errors="coerce").astype("Int64")
    tx_df["txn_ts"] = pd.to_datetime(tx_df["txn_ts"], errors="coerce", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    tx_df["amount"] = pd.to_numeric(tx_df["amount"], errors="coerce")
    tx_df["zip"] = normalize_zip(tx_df["zip"]) if "zip" in tx_df.columns else None
    tx_df = tx_df.dropna(subset=["txn_id", "user_id", "card_id", "merchant_id", "txn_ts", "amount", "mcc"])
    tx_df = tx_df.drop_duplicates(subset=["txn_id"])

    mcc_df = mcc_df[mcc_df["mcc"].isin(tx_df["mcc"].unique())].copy()

    merchants_df = merchants_raw.copy()
    merchants_df["merchant_id"] = pd.to_numeric(merchants_df["merchant_id"], errors="coerce").astype("Int64")
    merchants_df["mcc"] = pd.to_numeric(merchants_df["mcc"], errors="coerce").astype("Int64")
    merchants_df["zip"] = normalize_zip(merchants_df["zip"])
    merchants_df = merchants_df.dropna(subset=["merchant_id"]).drop_duplicates(subset=["merchant_id"])
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

    return {
        "users": users_df,
        "cards": cards_df,
        "categories": mcc_df,
        "merchants": merchants_df,
        "transactions": tx_df,
        "fraud": fraud_df,
    }


def df_to_records(df: pd.DataFrame, cols: List[str]) -> List[Dict[str, Any]]:
    data = df[cols].copy()
    data = data.where(pd.notna(data), None)
    return data.to_dict(orient="records")


def run_batched(session, query: str, rows: List[Dict[str, Any]], batch_size: int = BATCH_SIZE) -> None:
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        session.run(query, rows=batch).consume()


def node_count(session, label: str) -> int:
    result = session.run(f"MATCH (n:{label}) RETURN count(n) AS c")
    return int(result.single()["c"])


def rel_count(session, rel_type: str) -> int:
    result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS c")
    return int(result.single()["c"])


def ensure_constraints(session) -> None:
    statements = [
        "CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE",
        "CREATE CONSTRAINT card_id_unique IF NOT EXISTS FOR (c:Card) REQUIRE c.card_id IS UNIQUE",
        "CREATE CONSTRAINT category_mcc_unique IF NOT EXISTS FOR (cat:Category) REQUIRE cat.mcc IS UNIQUE",
        "CREATE CONSTRAINT merchant_id_unique IF NOT EXISTS FOR (m:Merchant) REQUIRE m.merchant_id IS UNIQUE",
        "CREATE CONSTRAINT txn_id_unique IF NOT EXISTS FOR (t:Transaction) REQUIRE t.txn_id IS UNIQUE",
    ]
    for stmt in statements:
        session.run(stmt).consume()


def migrate_nodes_if_empty(session, label: str, rows: List[Dict[str, Any]], query: str) -> None:
    count = node_count(session, label)
    if count > 0:
        print(f"SKIP {label}: already has {count} nodes.")
        return
    print(f"LOAD {label}: inserting {len(rows)} rows...")
    run_batched(session, query, rows)


def migrate_relationships_if_empty(session, rel_type: str, rows: List[Dict[str, Any]], query: str) -> None:
    count = rel_count(session, rel_type)
    if count > 0:
        print(f"SKIP {rel_type}: already has {count} relationships.")
        return
    print(f"LOAD {rel_type}: inserting from {len(rows)} rows...")
    run_batched(session, query, rows)


def main() -> None:
    print(f"DATA_DIR: {DATA_DIR}")
    print(f"SAMPLE_USERS: {SAMPLE_USERS if SAMPLE_USERS else 'ALL'}")
    print(f"NEO4J_URI: {NEO4J_URI}")
    print(f"NEO4J_DATABASE: {NEO4J_DATABASE}")

    src = load_sources()
    tables = build_tables(src)

    users_rows = df_to_records(
        tables["users"],
        [
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
        ],
    )
    cards_rows = df_to_records(
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
    categories_rows = df_to_records(tables["categories"], ["mcc", "description"])
    merchants_rows = df_to_records(
        tables["merchants"], ["merchant_id", "mcc", "merchant_city", "merchant_state", "zip"]
    )
    tx_rows = df_to_records(
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
    fraud_rows = df_to_records(tables["fraud"], ["txn_id", "is_fraud"])

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    target_database = NEO4J_DATABASE
    try:
        try:
            with driver.session(database=target_database) as probe_session:
                probe_session.run("RETURN 1").consume()
        except ClientError as exc:
            message = str(exc)
            if "DatabaseNotFound" in message or "does not exist" in message:
                print(
                    f"WARN: Database '{target_database}' not found on server. Falling back to 'neo4j'."
                )
                target_database = "neo4j"
            else:
                raise

        with driver.session(database=target_database) as session:
            ensure_constraints(session)

            migrate_nodes_if_empty(
                session,
                "User",
                users_rows,
                """
                UNWIND $rows AS row
                MERGE (u:User {user_id: row.user_id})
                SET u += row
                """,
            )
            migrate_nodes_if_empty(
                session,
                "Card",
                cards_rows,
                """
                UNWIND $rows AS row
                MERGE (c:Card {card_id: row.card_id})
                SET c += row
                """,
            )
            migrate_nodes_if_empty(
                session,
                "Category",
                categories_rows,
                """
                UNWIND $rows AS row
                MERGE (cat:Category {mcc: row.mcc})
                SET cat += row
                """,
            )
            migrate_nodes_if_empty(
                session,
                "Merchant",
                merchants_rows,
                """
                UNWIND $rows AS row
                MERGE (m:Merchant {merchant_id: row.merchant_id})
                SET m += row
                """,
            )
            migrate_nodes_if_empty(
                session,
                "Transaction",
                tx_rows,
                """
                UNWIND $rows AS row
                MERGE (t:Transaction {txn_id: row.txn_id})
                SET t += row
                """,
            )

            migrate_relationships_if_empty(
                session,
                "OWNS_CARD",
                cards_rows,
                """
                UNWIND $rows AS row
                MATCH (u:User {user_id: row.user_id})
                MATCH (c:Card {card_id: row.card_id})
                MERGE (u)-[:OWNS_CARD]->(c)
                """,
            )
            migrate_relationships_if_empty(
                session,
                "IN_CATEGORY",
                merchants_rows,
                """
                UNWIND $rows AS row
                MATCH (m:Merchant {merchant_id: row.merchant_id})
                MATCH (cat:Category {mcc: row.mcc})
                MERGE (m)-[:IN_CATEGORY]->(cat)
                """,
            )
            migrate_relationships_if_empty(
                session,
                "MADE",
                tx_rows,
                """
                UNWIND $rows AS row
                MATCH (u:User {user_id: row.user_id})
                MATCH (t:Transaction {txn_id: row.txn_id})
                MERGE (u)-[:MADE]->(t)
                """,
            )
            migrate_relationships_if_empty(
                session,
                "USED_IN",
                tx_rows,
                """
                UNWIND $rows AS row
                MATCH (c:Card {card_id: row.card_id})
                MATCH (t:Transaction {txn_id: row.txn_id})
                MERGE (c)-[:USED_IN]->(t)
                """,
            )
            migrate_relationships_if_empty(
                session,
                "AT_MERCHANT",
                tx_rows,
                """
                UNWIND $rows AS row
                MATCH (t:Transaction {txn_id: row.txn_id})
                MATCH (m:Merchant {merchant_id: row.merchant_id})
                MERGE (t)-[:AT_MERCHANT]->(m)
                """,
            )
            migrate_relationships_if_empty(
                session,
                "TXN_CATEGORY",
                tx_rows,
                """
                UNWIND $rows AS row
                MATCH (t:Transaction {txn_id: row.txn_id})
                MATCH (cat:Category {mcc: row.mcc})
                MERGE (t)-[:TXN_CATEGORY]->(cat)
                """,
            )

            print(f"LOAD fraud flags: setting {len(fraud_rows)} transaction labels...")
            run_batched(
                session,
                """
                UNWIND $rows AS row
                MATCH (t:Transaction {txn_id: row.txn_id})
                SET t.is_fraud = row.is_fraud
                """,
                fraud_rows,
            )

        print("Done")
    finally:
        driver.close()


if __name__ == "__main__":
    main()



