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



    def get_user_spending_graph(self, user_id: int) -> Dict[str, Any]:
        """Aggregate a user's spend by MCC category using the Neo4j graph."""
        query = """
        MATCH (u:User {user_id: $user_id})-[:MADE]->(t:Transaction)-[:TXN_CATEGORY]->(cat:Category)
        WITH cat.description AS category,
             sum(t.amount) AS amount,
             count(t) AS txn_count
        RETURN category, amount, txn_count
        ORDER BY amount DESC
        """
        settings = get_settings()
        driver = get_neo4j_driver()
        with driver.session(database=settings.neo4j_database) as session:
            rows = [record.data() for record in session.run(query, user_id=user_id)]

        total = sum(r["amount"] for r in rows) or 1.0

        categories = [
            {
                "category": r["category"] or "Other",
                "amount": round(float(r["amount"]), 2),
                "percentage": round(float(r["amount"]) / total * 100, 1),
                "transaction_count": int(r["txn_count"]),
            }
            for r in rows
        ]

        # Detect recurring merchants: visited ≥ 3 times (proxy for subscriptions/regulars)
        recurrence_query = """
        MATCH (u:User {user_id: $user_id})-[:MADE]->(t:Transaction)-[:AT_MERCHANT]->(m:Merchant)
        WITH m.merchant_id AS merchant_id, count(t) AS visits
        WHERE visits >= 3
        RETURN count(merchant_id) AS recurring_count
        """
        with driver.session(database=settings.neo4j_database) as session:
            rec_result = session.run(recurrence_query, user_id=user_id).single()
            recurring = int(rec_result["recurring_count"]) if rec_result else 0

        # Subscriptions: recurring merchants in known subscription-type MCCs
        sub_query = """
        MATCH (u:User {user_id: $user_id})-[:MADE]->(t:Transaction)-[:AT_MERCHANT]->(m:Merchant)
        WITH m.merchant_id AS merchant_id, count(t) AS visits
        WHERE visits >= 3
        MATCH (t2:Transaction {merchant_id: merchant_id})-[:TXN_CATEGORY]->(cat:Category)
        WHERE cat.description =~ '(?i).*(streaming|subscription|software|digital|membership).*'
        RETURN count(DISTINCT merchant_id) AS sub_count
        """
        with driver.session(database=settings.neo4j_database) as session:
            sub_result = session.run(sub_query, user_id=user_id).single()
            subscriptions = int(sub_result["sub_count"]) if sub_result else 0

        return {
            "user_id": user_id,
            "total_spend": round(total, 2),
            "period": "all-time",
            "categories": categories,
            "recurring_payments": recurring,
            "subscriptions": subscriptions,
        }

    def get_user_optimization(self, user_id: int) -> Dict[str, Any]:
        """Generate optimization suggestions from the user's graph data."""
        settings = get_settings()
        driver = get_neo4j_driver()
        suggestions = []

        # 1. Recurring merchants (potential unused subscriptions)
        recurrence_query = """
        MATCH (u:User {user_id: $user_id})-[:MADE]->(t:Transaction)-[:AT_MERCHANT]->(m:Merchant)
        WITH m.merchant_id AS merchant_id,
             count(t) AS visits,
             sum(t.amount) AS total_spent,
             avg(t.amount) AS avg_ticket
        WHERE visits >= 3
        RETURN merchant_id, visits, total_spent, avg_ticket
        ORDER BY total_spent DESC
        LIMIT 5
        """
        with driver.session(database=settings.neo4j_database) as session:
            recurring = [r.data() for r in session.run(recurrence_query, user_id=user_id)]

        if recurring:
            top = recurring[0]
            monthly_est = round(float(top["avg_ticket"]) * 12 / 12, 2)
            suggestions.append({
                "id": "opt-1",
                "title": "Review recurring payments",
                "description": (
                    f"You have {len(recurring)} merchant(s) with 3+ repeat transactions. "
                    f"The largest accounts for ${float(top['total_spent']):.0f} in total spend. "
                    "Review whether all are still needed."
                ),
                "estimated_savings": round(monthly_est, 2),
                "category": "Subscriptions",
                "priority": "high" if len(recurring) >= 3 else "medium",
            })

        # 2. Top overspent category relative to transaction count
        category_query = """
        MATCH (u:User {user_id: $user_id})-[:MADE]->(t:Transaction)-[:TXN_CATEGORY]->(cat:Category)
        WITH cat.description AS category, sum(t.amount) AS total, count(t) AS txn_count
        RETURN category, total, txn_count
        ORDER BY total DESC
        LIMIT 3
        """
        with driver.session(database=settings.neo4j_database) as session:
            top_cats = [r.data() for r in session.run(category_query, user_id=user_id)]

        if len(top_cats) >= 2:
            top_cat = top_cats[0]
            saving_est = round(float(top_cat["total"]) * 0.10, 2)
            suggestions.append({
                "id": "opt-2",
                "title": f"Reduce spend in {top_cat['category']}",
                "description": (
                    f"Your top spending category is '{top_cat['category']}' "
                    f"with ${float(top_cat['total']):.0f} across {int(top_cat['txn_count'])} transactions. "
                    "A 10% reduction could meaningfully improve your monthly budget."
                ),
                "estimated_savings": saving_est,
                "category": top_cat["category"] or "Other",
                "priority": "medium",
            })

        # 3. Exposure to high-fraud merchants
        fraud_exposure_query = """
        MATCH (u:User {user_id: $user_id})-[:MADE]->(t:Transaction)-[:AT_MERCHANT]->(m:Merchant)
        WITH m, count(t) AS user_visits
        MATCH (all_t:Transaction)-[:AT_MERCHANT]->(m)
        WITH m, user_visits,
             count(all_t) AS total_txns,
             sum(CASE WHEN coalesce(all_t.is_fraud, 0) = 1 THEN 1 ELSE 0 END) AS fraud_txns
        WITH m, user_visits, total_txns, fraud_txns,
             CASE WHEN total_txns = 0 THEN 0.0
                  ELSE toFloat(fraud_txns) / toFloat(total_txns) END AS fraud_rate
        WHERE fraud_rate > 0.05
        RETURN count(m) AS risky_merchant_count, sum(user_visits) AS risky_visits
        """
        with driver.session(database=settings.neo4j_database) as session:
            fraud_row = session.run(fraud_exposure_query, user_id=user_id).single()

        if fraud_row and int(fraud_row["risky_merchant_count"]) > 0:
            risky_count = int(fraud_row["risky_merchant_count"])
            suggestions.append({
                "id": "opt-3",
                "title": "Avoid high-risk merchants",
                "description": (
                    f"You have made transactions at {risky_count} merchant(s) with a fraud rate above 5%. "
                    "Consider using safer alternatives or enabling extra card verification for these."
                ),
                "estimated_savings": 0.0,
                "category": "Risk",
                "priority": "high" if risky_count >= 3 else "medium",
            })

        # 4. Card on dark web warning
        dark_web_query = """
        MATCH (u:User {user_id: $user_id})-[:OWNS_CARD]->(c:Card)
        WHERE c.card_on_dark_web = true
        RETURN count(c) AS dark_web_cards
        """
        with driver.session(database=settings.neo4j_database) as session:
            dw_row = session.run(dark_web_query, user_id=user_id).single()

        if dw_row and int(dw_row["dark_web_cards"]) > 0:
            suggestions.append({
                "id": "opt-4",
                "title": "Card credentials at risk",
                "description": (
                    f"{int(dw_row['dark_web_cards'])} of your card(s) appear in dark web data. "
                    "Request replacements immediately from your card issuer."
                ),
                "estimated_savings": 0.0,
                "category": "Security",
                "priority": "high",
            })

        total_savings = round(sum(s["estimated_savings"] for s in suggestions), 2)
        return {
            "user_id": user_id,
            "suggestions": suggestions,
            "total_estimated_savings": total_savings,
        }

    def get_user_policy_compliance(self, user_id: int) -> Dict[str, Any]:
        """Evaluate 4 financial policy rules from the user's graph and Postgres profile."""
        settings = get_settings()
        driver = get_neo4j_driver()

        # Fetch user financial profile from Neo4j here
        profile_query = """
        MATCH (u:User {user_id: $user_id})
        RETURN u.yearly_income AS yearly_income,
               u.total_debt AS total_debt,
               u.credit_score AS credit_score
        """
        with driver.session(database=settings.neo4j_database) as session:
            profile = session.run(profile_query, user_id=user_id).single()

        yearly_income = float(profile["yearly_income"] or 0) if profile else 0.0
        total_debt = float(profile["total_debt"] or 0) if profile else 0.0
        monthly_income = yearly_income / 12 if yearly_income > 0 else 1.0

        # Total spend from graph
        spend_query = """
        MATCH (u:User {user_id: $user_id})-[:MADE]->(t:Transaction)
        RETURN sum(t.amount) AS total_spend, count(t) AS txn_count
        """
        with driver.session(database=settings.neo4j_database) as session:
            spend_row = session.run(spend_query, user_id=user_id).single()

        total_spend = float(spend_row["total_spend"] or 0) if spend_row else 0.0
        txn_count = int(spend_row["txn_count"] or 0) if spend_row else 0

        # Fraud-merchant exposure rate
        fraud_query = """
        MATCH (u:User {user_id: $user_id})-[:MADE]->(t:Transaction)-[:AT_MERCHANT]->(m:Merchant)
        WITH m, count(t) AS user_visits
        MATCH (all_t:Transaction)-[:AT_MERCHANT]->(m)
        WITH m, user_visits,
             count(all_t) AS total_txns,
             sum(CASE WHEN coalesce(all_t.is_fraud, 0) = 1 THEN 1 ELSE 0 END) AS fraud_txns
        WITH CASE WHEN total_txns = 0 THEN 0.0
                  ELSE toFloat(fraud_txns) / toFloat(total_txns) END AS fraud_rate,
             user_visits
        WHERE fraud_rate > 0.05
        RETURN sum(user_visits) AS risky_visits
        """
        with driver.session(database=settings.neo4j_database) as session:
            fraud_row = session.run(fraud_query, user_id=user_id).single()

        risky_visits = int(fraud_row["risky_visits"] or 0) if fraud_row else 0
        fraud_exposure_pct = (risky_visits / txn_count * 100) if txn_count > 0 else 0.0

        # Rule 1: Spending-to-income ratio
        # Use total spend across all time vs yearly_income as a proxy.
        # Flag if monthly avg spend > 90% of monthly income.
        monthly_avg_spend = (total_spend / 12) if total_spend > 0 else 0.0
        spend_ratio = (monthly_avg_spend / monthly_income) if monthly_income > 0 else 0.0

        if spend_ratio <= 0.7:
            r1_status, r1_detail = "compliant", f"Monthly avg spend is {spend_ratio*100:.0f}% of monthly income — within healthy range."
        elif spend_ratio <= 0.9:
            r1_status, r1_detail = "warning", f"Monthly avg spend is {spend_ratio*100:.0f}% of monthly income — approaching limit."
        else:
            r1_status, r1_detail = "violation", f"Monthly avg spend is {spend_ratio*100:.0f}% of monthly income — exceeds safe threshold."

        # Rule 2: Debt-to-income ratio
        debt_ratio = (total_debt / yearly_income) if yearly_income > 0 else 0.0

        if debt_ratio <= 0.36:
            r2_status, r2_detail = "compliant", f"Debt-to-income ratio is {debt_ratio*100:.0f}% — healthy."
        elif debt_ratio <= 0.50:
            r2_status, r2_detail = "warning", f"Debt-to-income ratio is {debt_ratio*100:.0f}% — monitor closely."
        else:
            r2_status, r2_detail = "violation", f"Debt-to-income ratio is {debt_ratio*100:.0f}% — exceeds recommended 50%."

        # Rule 3: Fraud-merchant exposure
        if fraud_exposure_pct <= 5.0:
            r3_status, r3_detail = "compliant", f"{fraud_exposure_pct:.1f}% of your transactions are at high-fraud merchants — low exposure."
        elif fraud_exposure_pct <= 15.0:
            r3_status, r3_detail = "warning", f"{fraud_exposure_pct:.1f}% of your transactions are at high-fraud merchants — moderate exposure."
        else:
            r3_status, r3_detail = "violation", f"{fraud_exposure_pct:.1f}% of your transactions are at high-fraud merchants — high exposure."

        # Rule 4: Category concentration
        # Ensure spend isn't overly concentrated in a single category (>50% = warning, >70% = violation)
        cat_query = """
        MATCH (u:User {user_id: $user_id})-[:MADE]->(t:Transaction)-[:TXN_CATEGORY]->(cat:Category)
        WITH cat.description AS category, sum(t.amount) AS cat_spend
        RETURN cat_spend
        ORDER BY cat_spend DESC
        LIMIT 1
        """
        with driver.session(database=settings.neo4j_database) as session:
            cat_row = session.run(cat_query, user_id=user_id).single()

        top_cat_spend = float(cat_row["cat_spend"] or 0) if cat_row else 0.0
        concentration = (top_cat_spend / total_spend * 100) if total_spend > 0 else 0.0

        if concentration <= 50.0:
            r4_status, r4_detail = "compliant", f"Top category accounts for {concentration:.0f}% of spend — well diversified."
        elif concentration <= 70.0:
            r4_status, r4_detail = "warning", f"Top category accounts for {concentration:.0f}% of spend — moderately concentrated."
        else:
            r4_status, r4_detail = "violation", f"Top category accounts for {concentration:.0f}% of spend — overly concentrated."

        rules = [
            {"id": "rule-1", "name": "Spending-to-Income Ratio", "description": "Monthly average spend should not exceed 90% of monthly income.", "status": r1_status, "detail": r1_detail},
            {"id": "rule-2", "name": "Debt-to-Income Ratio", "description": "Total debt should not exceed 50% of annual income.", "status": r2_status, "detail": r2_detail},
            {"id": "rule-3", "name": "Fraud Merchant Exposure", "description": "Less than 15% of transactions should occur at high-fraud-rate merchants.", "status": r3_status, "detail": r3_detail},
            {"id": "rule-4", "name": "Spending Concentration", "description": "No single category should account for more than 70% of total spend.", "status": r4_status, "detail": r4_detail},
        ]

        status_score = {"compliant": 25, "warning": 12, "violation": 0}
        score = sum(status_score[r["status"]] for r in rules)

        violations = [r for r in rules if r["status"] == "violation"]
        warnings = [r for r in rules if r["status"] == "warning"]
        if violations:
            overall_status = "violation"
        elif warnings:
            overall_status = "warning"
        else:
            overall_status = "compliant"

        return {
            "user_id": user_id,
            "overall_status": overall_status,
            "score": score,
            "rules": rules,
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
