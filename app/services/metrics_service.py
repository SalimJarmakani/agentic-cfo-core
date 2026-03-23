from threading import Lock
from typing import Any, Dict, Optional

from psycopg2.extras import RealDictCursor

from app.db.postgres import get_postgres_connection, get_postgres_cursor


class MetricsService:
    _schema_ready = False
    _schema_lock = Lock()

    def ensure_metrics_schema(self) -> None:
        if self.__class__._schema_ready:
            return

        with self.__class__._schema_lock:
            if self.__class__._schema_ready:
                return

            with get_postgres_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS api_request_logs (
                          request_log_id BIGSERIAL PRIMARY KEY,
                          method TEXT NOT NULL,
                          path TEXT NOT NULL,
                          status_code INTEGER NOT NULL,
                          duration_ms DOUBLE PRECISION NOT NULL,
                          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        );
                        CREATE INDEX IF NOT EXISTS idx_api_request_logs_created ON api_request_logs(created_at DESC);
                        CREATE INDEX IF NOT EXISTS idx_api_request_logs_path ON api_request_logs(path);

                        CREATE TABLE IF NOT EXISTS validated_query_evaluations (
                          evaluation_id BIGSERIAL PRIMARY KEY,
                          question TEXT NOT NULL,
                          expected_answer TEXT NOT NULL,
                          actual_answer TEXT NOT NULL,
                          is_correct BOOLEAN,
                          evaluator TEXT,
                          notes TEXT,
                          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        );
                        CREATE INDEX IF NOT EXISTS idx_validated_query_evaluations_created
                          ON validated_query_evaluations(created_at DESC);

                        CREATE TABLE IF NOT EXISTS recommendation_feedback (
                          feedback_id BIGSERIAL PRIMARY KEY,
                          workflow_run_id BIGINT NOT NULL REFERENCES workflow_runs(workflow_run_id) ON DELETE CASCADE,
                          recommendation_stage TEXT NOT NULL
                            CHECK (recommendation_stage IN ('analysis', 'planning', 'policy', 'explanation')),
                          usefulness_rating INTEGER NOT NULL CHECK (usefulness_rating BETWEEN 1 AND 5),
                          clarity_rating INTEGER CHECK (clarity_rating BETWEEN 1 AND 5),
                          adopted BOOLEAN,
                          evaluator TEXT,
                          comments TEXT,
                          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        );
                        CREATE INDEX IF NOT EXISTS idx_recommendation_feedback_workflow
                          ON recommendation_feedback(workflow_run_id, created_at DESC);
                        """
                    )

            self.__class__._schema_ready = True

    def record_api_request(self, method: str, path: str, status_code: int, duration_ms: float) -> None:
        self.ensure_metrics_schema()
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO api_request_logs (method, path, status_code, duration_ms)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (method, path, status_code, duration_ms),
                )

    def create_validated_query_evaluation(
        self,
        question: str,
        expected_answer: str,
        actual_answer: str,
        is_correct: Optional[bool],
        evaluator: Optional[str],
        notes: Optional[str],
    ) -> Dict[str, Any]:
        self.ensure_metrics_schema()
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO validated_query_evaluations (
                        question,
                        expected_answer,
                        actual_answer,
                        is_correct,
                        evaluator,
                        notes
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING
                        evaluation_id,
                        question,
                        expected_answer,
                        actual_answer,
                        is_correct,
                        evaluator,
                        notes,
                        created_at::text AS created_at
                    """,
                    (question, expected_answer, actual_answer, is_correct, evaluator, notes),
                )
                return dict(cur.fetchone())

    def create_recommendation_feedback(
        self,
        workflow_run_id: int,
        recommendation_stage: str,
        usefulness_rating: int,
        clarity_rating: Optional[int],
        adopted: Optional[bool],
        evaluator: Optional[str],
        comments: Optional[str],
    ) -> Dict[str, Any]:
        self.ensure_metrics_schema()
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO recommendation_feedback (
                        workflow_run_id,
                        recommendation_stage,
                        usefulness_rating,
                        clarity_rating,
                        adopted,
                        evaluator,
                        comments
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING
                        feedback_id,
                        workflow_run_id,
                        recommendation_stage,
                        usefulness_rating,
                        clarity_rating,
                        adopted,
                        evaluator,
                        comments,
                        created_at::text AS created_at
                    """,
                    (
                        workflow_run_id,
                        recommendation_stage,
                        usefulness_rating,
                        clarity_rating,
                        adopted,
                        evaluator,
                        comments,
                    ),
                )
                return dict(cur.fetchone())

    def get_summary(self) -> Dict[str, Any]:
        self.ensure_metrics_schema()
        workflow = self._get_workflow_metrics()
        policy = self._get_policy_metrics()
        response = {
            **self._get_analytics_accuracy_metrics(),
            **workflow,
            **policy,
            **self._get_response_time_metrics(),
            **self._get_human_feedback_metrics(),
        }
        response["average_stage_response_time_ms"] = self._get_stage_response_time_metrics()
        return response

    def get_agent_token_usage(self) -> Dict[str, Any]:
        self.ensure_metrics_schema()
        with get_postgres_cursor() as cur:
            cur.execute(
                """
                SELECT
                    step_name,
                    COUNT(*) FILTER (
                        WHERE output_payload IS NOT NULL
                          AND output_payload ? 'input_tokens'
                    )::int AS runs_with_input_tokens,
                    AVG((output_payload->>'input_tokens')::float) FILTER (
                        WHERE output_payload IS NOT NULL
                          AND output_payload ? 'input_tokens'
                    ) AS avg_input_tokens
                FROM workflow_steps
                WHERE status = 'completed'
                GROUP BY step_name
                """
            )
            rows = cur.fetchall() or []

        averages: Dict[str, Optional[float]] = {
            "analysis": None,
            "planning": None,
            "policy": None,
            "explanation": None,
        }
        counts: Dict[str, int] = {
            "analysis": 0,
            "planning": 0,
            "policy": 0,
            "explanation": 0,
        }

        for row in rows:
            step_name = str(row.get("step_name") or "")
            if step_name not in averages:
                continue
            counts[step_name] = int(row.get("runs_with_input_tokens") or 0)
            averages[step_name] = self._round_nullable(row.get("avg_input_tokens"))

        return {
            "average_input_tokens_by_agent": averages,
            "runs_with_input_tokens_by_agent": counts,
        }

    def _get_analytics_accuracy_metrics(self) -> Dict[str, Any]:
        with get_postgres_cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE is_correct IS NOT NULL)::int AS total_evaluations,
                    AVG(CASE WHEN is_correct THEN 1.0 ELSE 0.0 END) AS accuracy_rate
                FROM validated_query_evaluations
                """
            )
            row = cur.fetchone() or {}
            return {
                "analytics_accuracy_rate": self._round_nullable_percentage(row.get("accuracy_rate")),
                "analytics_accuracy_total_evaluations": int(row.get("total_evaluations") or 0),
            }

    def _get_workflow_metrics(self) -> Dict[str, Any]:
        with get_postgres_cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)::int AS total_runs,
                    COUNT(*) FILTER (WHERE status = 'completed')::int AS completed_runs,
                    COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_runs,
                    COUNT(*) FILTER (WHERE status NOT IN ('completed', 'failed'))::int AS in_progress_runs,
                    AVG(
                        CASE
                            WHEN status = 'completed'
                            THEN EXTRACT(EPOCH FROM (updated_at - created_at)) * 1000.0
                            ELSE NULL
                        END
                    ) AS avg_completion_ms
                FROM workflow_runs
                """
            )
            row = cur.fetchone() or {}
            total_runs = int(row.get("total_runs") or 0)
            completed_runs = int(row.get("completed_runs") or 0)
            return {
                "workflow_completion_rate": self._safe_rate(completed_runs, total_runs),
                "workflow_total_runs": total_runs,
                "workflow_completed_runs": completed_runs,
                "workflow_failed_runs": int(row.get("failed_runs") or 0),
                "workflow_in_progress_runs": int(row.get("in_progress_runs") or 0),
                "average_workflow_completion_time_ms": self._round_nullable(row.get("avg_completion_ms")),
            }

    def _get_policy_metrics(self) -> Dict[str, Any]:
        with get_postgres_cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)::int AS total_reviews,
                    COUNT(*) FILTER (
                        WHERE
                            COALESCE((output_payload->>'requires_human_review')::boolean, false) = true
                            OR COALESCE((output_payload->>'approved')::boolean, true) = false
                    )::int AS intervention_count,
                    COUNT(*) FILTER (
                        WHERE COALESCE(output_payload->>'user_policy_status', '') = 'compliant'
                    )::int AS compliant_count
                FROM workflow_steps
                WHERE step_name = 'policy'
                  AND status = 'completed'
                  AND output_payload IS NOT NULL
                """
            )
            row = cur.fetchone() or {}
            total_reviews = int(row.get("total_reviews") or 0)
            intervention_count = int(row.get("intervention_count") or 0)
            compliant_count = int(row.get("compliant_count") or 0)
            return {
                "policy_intervention_rate": self._safe_rate(intervention_count, total_reviews),
                "policy_intervention_total_reviews": total_reviews,
                "policy_intervention_count": intervention_count,
                "policy_compliance_rate": self._safe_rate(compliant_count, total_reviews),
                "policy_compliance_total_reviews": total_reviews,
                "policy_compliant_count": compliant_count,
            }

    def _get_response_time_metrics(self) -> Dict[str, Any]:
        with get_postgres_cursor() as cur:
            cur.execute(
                """
                SELECT AVG(duration_ms) AS avg_api_response_time_ms
                FROM api_request_logs
                WHERE path LIKE '/api/v1/%'
                  AND path NOT LIKE '/api/v1/metrics%'
                """
            )
            row = cur.fetchone() or {}
            return {
                "average_api_response_time_ms": self._round_nullable(row.get("avg_api_response_time_ms")),
            }

    def _get_stage_response_time_metrics(self) -> Dict[str, float]:
        with get_postgres_cursor() as cur:
            cur.execute(
                """
                SELECT
                    step_name,
                    AVG(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000.0) AS avg_stage_ms
                FROM workflow_steps
                WHERE started_at IS NOT NULL
                  AND completed_at IS NOT NULL
                GROUP BY step_name
                """
            )
            rows = cur.fetchall() or []

        metrics = {step: 0.0 for step in ("analysis", "planning", "policy", "explanation")}
        for row in rows:
            step_name = str(row.get("step_name") or "")
            if step_name in metrics:
                metrics[step_name] = self._round_nullable(row.get("avg_stage_ms")) or 0.0
        return metrics

    def _get_human_feedback_metrics(self) -> Dict[str, Any]:
        with get_postgres_cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)::int AS total_reviews,
                    AVG(usefulness_rating::float) AS avg_usefulness
                FROM recommendation_feedback
                """
            )
            row = cur.fetchone() or {}
            return {
                "human_recommendation_usefulness_avg": self._round_nullable(row.get("avg_usefulness")),
                "human_recommendation_usefulness_total_reviews": int(row.get("total_reviews") or 0),
            }

    @staticmethod
    def _safe_rate(numerator: int, denominator: int) -> Optional[float]:
        if denominator <= 0:
            return None
        return round((numerator / denominator) * 100, 2)

    @staticmethod
    def _round_nullable(value: Any) -> Optional[float]:
        if value is None:
            return None
        return round(float(value), 2)

    @staticmethod
    def _round_nullable_percentage(value: Any) -> Optional[float]:
        if value is None:
            return None
        return round(float(value) * 100, 2)
