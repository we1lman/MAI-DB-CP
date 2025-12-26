"""views + materialized views + performance indexes

Revision ID: 0003_views_mviews_indexes
Revises: 0002_triggers_audit_status_next_due
Create Date: 2025-12-26
"""

from __future__ import annotations

from alembic import op

revision = "0003_views_mviews_indexes"
down_revision = "0002_triggers_audit_status_next_due"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        -- ===== Performance indexes (base tables) =====
        CREATE INDEX IF NOT EXISTS ix_instrument_org_unit_id ON metrology.instrument(org_unit_id);
        CREATE INDEX IF NOT EXISTS ix_instrument_location_id ON metrology.instrument(location_id);
        CREATE INDEX IF NOT EXISTS ix_instrument_model_id ON metrology.instrument(instrument_model_id);

        CREATE INDEX IF NOT EXISTS ix_check_plan_due_date ON metrology.check_plan(due_date);
        CREATE INDEX IF NOT EXISTS ix_check_event_instrument_date ON metrology.check_event(instrument_id, check_date DESC);
        CREATE INDEX IF NOT EXISTS ix_check_event_next_due_date ON metrology.check_event(next_due_date) WHERE next_due_date IS NOT NULL;

        CREATE INDEX IF NOT EXISTS ix_audit_log_at ON metrology.audit_log(at DESC);
        CREATE INDEX IF NOT EXISTS ix_audit_log_table_at ON metrology.audit_log(table_name, at DESC);

        -- ===== Views =====
        CREATE OR REPLACE VIEW metrology.v_instrument_check_next_due AS
        WITH last_success AS (
          SELECT
            ce.instrument_id,
            ce.check_type_id,
            max(ce.check_date) AS last_check_date
          FROM metrology.check_event ce
          JOIN metrology.check_result_status rs ON rs.id = ce.result_status_id
          WHERE rs.is_success = true
          GROUP BY ce.instrument_id, ce.check_type_id
        )
        SELECT
          i.id AS instrument_id,
          i.inventory_no,
          i.serial_no,
          i.org_unit_id,
          i.location_id,
          ct.id AS check_type_id,
          ct.code AS check_type_code,
          ct.name AS check_type_name,
          ls.last_check_date,
          ce.next_due_date,
          (ce.next_due_date - current_date) AS days_to_due,
          ce.protocol_no,
          ce.lab_id,
          ce.specialist_id
        FROM last_success ls
        JOIN metrology.instrument i ON i.id = ls.instrument_id
        JOIN metrology.check_type ct ON ct.id = ls.check_type_id
        JOIN metrology.check_event ce
          ON ce.instrument_id = ls.instrument_id
         AND ce.check_type_id = ls.check_type_id
         AND ce.check_date = ls.last_check_date
        ;

        CREATE OR REPLACE VIEW metrology.v_instrument_next_due AS
        SELECT
          instrument_id,
          inventory_no,
          serial_no,
          org_unit_id,
          location_id,
          min(next_due_date) AS next_due_date,
          min(days_to_due) AS days_to_due
        FROM metrology.v_instrument_check_next_due
        WHERE next_due_date IS NOT NULL
        GROUP BY instrument_id, inventory_no, serial_no, org_unit_id, location_id;

        -- ===== Materialized views =====
        DROP MATERIALIZED VIEW IF EXISTS metrology.mv_instruments_due_30d;
        CREATE MATERIALIZED VIEW metrology.mv_instruments_due_30d AS
        SELECT *
        FROM metrology.v_instrument_check_next_due
        WHERE next_due_date IS NOT NULL
          AND next_due_date >= current_date
          AND next_due_date < (current_date + 30);

        DROP MATERIALIZED VIEW IF EXISTS metrology.mv_instruments_overdue;
        CREATE MATERIALIZED VIEW metrology.mv_instruments_overdue AS
        SELECT *
        FROM metrology.v_instrument_check_next_due
        WHERE next_due_date IS NOT NULL
          AND next_due_date < current_date;

        -- For CONCURRENTLY refresh (requires unique index)
        CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_due_30d
          ON metrology.mv_instruments_due_30d(instrument_id, check_type_id);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_overdue
          ON metrology.mv_instruments_overdue(instrument_id, check_type_id);

        -- ===== Refresh procedure (non-concurrent) =====
        -- Note: REFRESH ... CONCURRENTLY cannot be used inside a function/procedure transaction block.
        CREATE OR REPLACE PROCEDURE metrology.sp_refresh_due_mviews()
        LANGUAGE plpgsql
        AS $$
        BEGIN
          REFRESH MATERIALIZED VIEW metrology.mv_instruments_due_30d;
          REFRESH MATERIALIZED VIEW metrology.mv_instruments_overdue;
        END;
        $$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP PROCEDURE IF EXISTS metrology.sp_refresh_due_mviews();

        DROP MATERIALIZED VIEW IF EXISTS metrology.mv_instruments_due_30d;
        DROP MATERIALIZED VIEW IF EXISTS metrology.mv_instruments_overdue;

        DROP VIEW IF EXISTS metrology.v_instrument_next_due;
        DROP VIEW IF EXISTS metrology.v_instrument_check_next_due;

        DROP INDEX IF EXISTS metrology.ix_instrument_org_unit_id;
        DROP INDEX IF EXISTS metrology.ix_instrument_location_id;
        DROP INDEX IF EXISTS metrology.ix_instrument_model_id;
        DROP INDEX IF EXISTS metrology.ix_check_plan_due_date;
        DROP INDEX IF EXISTS metrology.ix_check_event_instrument_date;
        DROP INDEX IF EXISTS metrology.ix_check_event_next_due_date;
        DROP INDEX IF EXISTS metrology.ix_audit_log_at;
        DROP INDEX IF EXISTS metrology.ix_audit_log_table_at;
        """
    )


