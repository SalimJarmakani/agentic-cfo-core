import json
import logging
from threading import Lock
from typing import Any, Dict, Optional

from psycopg2.extras import Json, RealDictCursor

from app.db.postgres import get_postgres_connection

logger = logging.getLogger(__name__)


class WorkflowServiceError(RuntimeError):
    pass


class WorkflowNotFoundError(WorkflowServiceError):
    pass


class WorkflowService:
    _schema_ready = False
    _schema_lock = Lock()

    def create_workflow_run(self, user_id: int, question: str) -> Dict[str, Any]:
        self.ensure_workflow_schema()
        logger.info("Creating workflow run for user_id=%s", user_id)
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO workflow_runs (user_id, question, status, current_stage)
                    VALUES (%s, %s, 'running', 'analysis')
                    RETURNING workflow_run_id, user_id, question, status, current_stage, created_at, updated_at
                    """,
                    (user_id, question),
                )
                workflow = dict(cur.fetchone())

                cur.execute(
                    """
                    INSERT INTO workflow_steps (workflow_run_id, step_name, status)
                    VALUES
                        (%s, 'analysis', 'pending'),
                        (%s, 'planning', 'pending'),
                        (%s, 'policy', 'pending')
                    """,
                    (
                        workflow["workflow_run_id"],
                        workflow["workflow_run_id"],
                        workflow["workflow_run_id"],
                    ),
                )

        workflow_run_id = int(workflow["workflow_run_id"])
        logger.info("Created workflow run workflow_run_id=%s for user_id=%s", workflow_run_id, user_id)
        return self.get_workflow(workflow_run_id)

    def ensure_workflow_schema(self) -> None:
        if self.__class__._schema_ready:
            return

        with self.__class__._schema_lock:
            if self.__class__._schema_ready:
                return

            logger.info("Ensuring workflow schema is ready")
            with get_postgres_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        DO $$
                        DECLARE
                            runs_constraint TEXT;
                        BEGIN
                            SELECT con.conname
                            INTO runs_constraint
                            FROM pg_constraint con
                            JOIN pg_class rel ON rel.oid = con.conrelid
                            WHERE rel.relname = 'workflow_runs'
                              AND con.contype = 'c'
                              AND pg_get_constraintdef(con.oid) LIKE '%current_stage%';

                            IF runs_constraint IS NOT NULL THEN
                                EXECUTE format('ALTER TABLE workflow_runs DROP CONSTRAINT %I', runs_constraint);
                            END IF;

                            ALTER TABLE workflow_runs
                            ADD CONSTRAINT workflow_runs_current_stage_check
                            CHECK (current_stage IN ('analysis', 'planning', 'policy', 'done', 'failed'));
                        EXCEPTION
                            WHEN duplicate_object THEN NULL;
                        END $$;
                        """
                    )
                    cur.execute(
                        """
                        DO $$
                        DECLARE
                            steps_constraint TEXT;
                        BEGIN
                            SELECT con.conname
                            INTO steps_constraint
                            FROM pg_constraint con
                            JOIN pg_class rel ON rel.oid = con.conrelid
                            WHERE rel.relname = 'workflow_steps'
                              AND con.contype = 'c'
                              AND pg_get_constraintdef(con.oid) LIKE '%step_name%';

                            IF steps_constraint IS NOT NULL THEN
                                EXECUTE format('ALTER TABLE workflow_steps DROP CONSTRAINT %I', steps_constraint);
                            END IF;

                            ALTER TABLE workflow_steps
                            ADD CONSTRAINT workflow_steps_step_name_check
                            CHECK (step_name IN ('analysis', 'planning', 'policy'));
                        EXCEPTION
                            WHEN duplicate_object THEN NULL;
                        END $$;
                        """
                    )
            self.__class__._schema_ready = True

    def ensure_workflow_steps(self, workflow_run_id: int) -> None:
        self.ensure_workflow_schema()
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO workflow_steps (workflow_run_id, step_name, status)
                    VALUES
                        (%s, 'analysis', 'pending'),
                        (%s, 'planning', 'pending'),
                        (%s, 'policy', 'pending')
                    ON CONFLICT (workflow_run_id, step_name) DO NOTHING
                    """,
                    (workflow_run_id, workflow_run_id, workflow_run_id),
                )

    def list_workflows(self, user_id: int, limit: int = 20) -> list[Dict[str, Any]]:
        safe_limit = max(1, min(limit, 100))
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        workflow_run_id,
                        user_id,
                        question,
                        status,
                        current_stage,
                        created_at::text AS created_at,
                        updated_at::text AS updated_at
                    FROM workflow_runs
                    WHERE user_id = %s
                    ORDER BY updated_at DESC, workflow_run_id DESC
                    LIMIT %s
                    """,
                    (user_id, safe_limit),
                )
                return [dict(row) for row in cur.fetchall()]

    def get_workflow(self, workflow_run_id: int) -> Dict[str, Any]:
        self.ensure_workflow_schema()
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        workflow_run_id,
                        user_id,
                        question,
                        status,
                        current_stage,
                        created_at::text AS created_at,
                        updated_at::text AS updated_at
                    FROM workflow_runs
                    WHERE workflow_run_id = %s
                    """,
                    (workflow_run_id,),
                )
                workflow = cur.fetchone()
                if not workflow:
                    raise WorkflowNotFoundError(f"Workflow {workflow_run_id} was not found.")

        self.ensure_workflow_steps(workflow_run_id)

        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        workflow_step_id,
                        workflow_run_id,
                        step_name,
                        status,
                        input_payload,
                        output_payload,
                        error_message,
                        started_at::text AS started_at,
                        completed_at::text AS completed_at,
                        created_at::text AS created_at,
                        updated_at::text AS updated_at
                    FROM workflow_steps
                    WHERE workflow_run_id = %s
                    ORDER BY
                        CASE step_name
                            WHEN 'analysis' THEN 1
                            WHEN 'planning' THEN 2
                            WHEN 'policy' THEN 3
                            ELSE 99
                        END
                    """,
                    (workflow_run_id,),
                )
                steps = [dict(row) for row in cur.fetchall()]

        workflow_data = dict(workflow)
        workflow_data["steps"] = steps
        return workflow_data

    def get_step(self, workflow_run_id: int, step_name: str) -> Dict[str, Any]:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        workflow_step_id,
                        workflow_run_id,
                        step_name,
                        status,
                        input_payload,
                        output_payload,
                        error_message,
                        started_at::text AS started_at,
                        completed_at::text AS completed_at
                    FROM workflow_steps
                    WHERE workflow_run_id = %s AND step_name = %s
                    """,
                    (workflow_run_id, step_name),
                )
                row = cur.fetchone()
                if not row:
                    raise WorkflowNotFoundError(
                        f"Workflow step '{step_name}' for workflow {workflow_run_id} was not found."
                    )
                return dict(row)

    def start_step(
        self,
        workflow_run_id: int,
        step_name: str,
        input_payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._update_step(
            workflow_run_id=workflow_run_id,
            step_name=step_name,
            status="running",
            input_payload=input_payload,
            output_payload=None,
            error_message=None,
            set_started=True,
            set_completed=False,
        )

    def complete_step(
        self,
        workflow_run_id: int,
        step_name: str,
        output_payload: Dict[str, Any],
    ) -> None:
        self._update_step(
            workflow_run_id=workflow_run_id,
            step_name=step_name,
            status="completed",
            input_payload=None,
            output_payload=output_payload,
            error_message=None,
            set_started=False,
            set_completed=True,
        )

    def fail_step(self, workflow_run_id: int, step_name: str, error_message: str) -> None:
        self._update_step(
            workflow_run_id=workflow_run_id,
            step_name=step_name,
            status="failed",
            input_payload=None,
            output_payload=None,
            error_message=error_message,
            set_started=False,
            set_completed=True,
        )

    def update_workflow_status(self, workflow_run_id: int, status: str, current_stage: str) -> None:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    UPDATE workflow_runs
                    SET status = %s,
                        current_stage = %s,
                        updated_at = NOW()
                    WHERE workflow_run_id = %s
                    """,
                    (status, current_stage, workflow_run_id),
                )
                if cur.rowcount == 0:
                    raise WorkflowNotFoundError(f"Workflow {workflow_run_id} was not found.")

    def _update_step(
        self,
        workflow_run_id: int,
        step_name: str,
        status: str,
        input_payload: Optional[Dict[str, Any]],
        output_payload: Optional[Dict[str, Any]],
        error_message: Optional[str],
        set_started: bool,
        set_completed: bool,
    ) -> None:
        started_sql = "started_at = NOW()," if set_started else ""
        completed_sql = "completed_at = NOW()," if set_completed else ""

        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    UPDATE workflow_steps
                    SET status = %s,
                        input_payload = COALESCE(%s, input_payload),
                        output_payload = COALESCE(%s, output_payload),
                        error_message = %s,
                        {started_sql}
                        {completed_sql}
                        updated_at = NOW()
                    WHERE workflow_run_id = %s AND step_name = %s
                    """,
                    (
                        status,
                        Json(self._to_json_value(input_payload)) if input_payload is not None else None,
                        Json(self._to_json_value(output_payload)) if output_payload is not None else None,
                        error_message,
                        workflow_run_id,
                        step_name,
                    ),
                )
                if cur.rowcount == 0:
                    raise WorkflowNotFoundError(
                        f"Workflow step '{step_name}' for workflow {workflow_run_id} was not found."
                    )

    @staticmethod
    def _to_json_value(value: Dict[str, Any]) -> Dict[str, Any]:
        return json.loads(json.dumps(value, default=str))
