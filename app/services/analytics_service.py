from typing import Any, Dict, List

from app.core.config import get_settings
from app.db.neo4j import get_neo4j_driver
from app.db.postgres import get_postgres_cursor


class AnalyticsService:
    def get_users_paginated(self, page: int, page_size: int) -> Dict[str, Any]:
        offset = (page - 1) * page_size
        with get_postgres_cursor() as cur:
            cur.execute("SELECT COUNT(*)::int AS total FROM users")
            total = int(cur.fetchone()["total"])

            cur.execute(
                """
                SELECT
                    user_id,
                    current_age,
                    retirement_age,
                    birth_year,
                    birth_month,
                    gender,
                    address,
                    latitude,
                    longitude,
                    per_capita_income,
                    yearly_income,
                    total_debt,
                    credit_score,
                    num_credit_cards
                FROM users
                ORDER BY user_id ASC
                LIMIT %s OFFSET %s
                """,
                (page_size, offset),
            )
            rows = [dict(r) for r in cur.fetchall()]

        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "items": rows,
        }

    def get_recent_transactions(self, limit: int | None = None) -> List[Dict[str, Any]]:
        settings = get_settings()
        safe_limit = limit or settings.default_recent_tx_limit
        with get_postgres_cursor() as cur:
            cur.execute(
                """
                SELECT
                    txn_id, user_id, card_id, merchant_id, txn_ts, amount, mcc
                FROM transactions
                ORDER BY txn_ts DESC
                LIMIT %s
                """,
                (safe_limit,),
            )
            return [dict(r) for r in cur.fetchall()]

    def get_user_spending_summary(self, user_id: int) -> Dict[str, Any]:
        with get_postgres_cursor() as cur:
            cur.execute(
                """
                SELECT
                    user_id,
                    COUNT(*)::int AS txn_count,
                    COALESCE(SUM(amount), 0)::float AS total_spend,
                    COALESCE(AVG(amount), 0)::float AS avg_ticket,
                    MIN(txn_ts)::text AS first_txn_ts,
                    MAX(txn_ts)::text AS last_txn_ts
                FROM transactions
                WHERE user_id = %s
                GROUP BY user_id
                """,
                (user_id,),
            )
            row = cur.fetchone()
            if row:
                return dict(row)
            return {
                "user_id": user_id,
                "txn_count": 0,
                "total_spend": 0.0,
                "avg_ticket": 0.0,
                "first_txn_ts": None,
                "last_txn_ts": None,
            }

    def get_risky_merchants(self, limit: int | None = None) -> List[Dict[str, Any]]:
        settings = get_settings()
        safe_limit = limit or settings.default_risky_merchants_limit
        query = """
        MATCH (t:Transaction)-[:AT_MERCHANT]->(m:Merchant)
        WITH m, count(t) AS txn_count, sum(CASE WHEN coalesce(t.is_fraud, 0) = 1 THEN 1 ELSE 0 END) AS fraud_count
        WITH m, txn_count, fraud_count,
             CASE WHEN txn_count = 0 THEN 0.0 ELSE toFloat(fraud_count) / toFloat(txn_count) END AS fraud_rate
        RETURN
            m.merchant_id AS merchant_id,
            m.merchant_city AS merchant_city,
            m.merchant_state AS merchant_state,
            txn_count,
            fraud_count,
            fraud_rate
        ORDER BY fraud_rate DESC, fraud_count DESC, txn_count DESC
        LIMIT $limit
        """
        settings = get_settings()
        driver = get_neo4j_driver()
        with driver.session(database=settings.neo4j_database) as session:
            result = session.run(query, limit=safe_limit)
            return [record.data() for record in result]
