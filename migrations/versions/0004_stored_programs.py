"""stored programs: register event, decommission instrument, generate plan

Revision ID: 0004_stored_programs
Revises: 0003_views_mviews_indexes
Create Date: 2025-12-26
"""

from __future__ import annotations

from alembic import op

revision = "0004_stored_programs"
down_revision = "0003_views_mviews_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        -- Prevent duplicate plans for the same instrument/type/date
        ALTER TABLE metrology.check_plan
          ADD CONSTRAINT uq_check_plan UNIQUE (instrument_id, check_type_id, due_date);

        -- ===== Helper lookups by code =====
        CREATE OR REPLACE FUNCTION metrology.fn_check_plan_status_id(p_code text)
        RETURNS uuid
        LANGUAGE sql
        AS $$
          SELECT id FROM metrology.check_plan_status WHERE code = p_code;
        $$;

        CREATE OR REPLACE FUNCTION metrology.fn_instrument_status_id(p_code text)
        RETURNS uuid
        LANGUAGE sql
        AS $$
          SELECT id FROM metrology.instrument_status WHERE code = p_code;
        $$;

        CREATE OR REPLACE FUNCTION metrology.fn_check_result_status_id(p_code text)
        RETURNS uuid
        LANGUAGE sql
        AS $$
          SELECT id FROM metrology.check_result_status WHERE code = p_code;
        $$;

        -- ===== Register check event (plan/fact, docs) =====
        CREATE OR REPLACE FUNCTION metrology.fn_register_check_event(
          p_instrument_id uuid,
          p_check_type_id uuid,
          p_check_date date,
          p_result_code text,
          p_lab_id uuid,
          p_specialist_id uuid DEFAULT NULL,
          p_check_plan_id uuid DEFAULT NULL,
          p_protocol_no text DEFAULT NULL,
          p_notes text DEFAULT NULL,
          p_document_ids uuid[] DEFAULT NULL
        )
        RETURNS uuid
        LANGUAGE plpgsql
        AS $$
        DECLARE
          v_result_id uuid;
          v_event_id uuid;
        BEGIN
          v_result_id := metrology.fn_check_result_status_id(p_result_code);
          IF v_result_id IS NULL THEN
            RAISE EXCEPTION 'Unknown result code: %', p_result_code;
          END IF;

          INSERT INTO metrology.check_event(
            instrument_id,
            check_plan_id,
            check_type_id,
            lab_id,
            specialist_id,
            check_date,
            result_status_id,
            protocol_no,
            notes
          )
          VALUES (
            p_instrument_id,
            p_check_plan_id,
            p_check_type_id,
            p_lab_id,
            p_specialist_id,
            p_check_date,
            v_result_id,
            p_protocol_no,
            p_notes
          )
          RETURNING id INTO v_event_id;

          IF p_document_ids IS NOT NULL THEN
            INSERT INTO metrology.check_event_document(check_event_id, document_id)
            SELECT v_event_id, unnest(p_document_ids);
          END IF;

          IF p_check_plan_id IS NOT NULL THEN
            UPDATE metrology.check_plan
              SET status_id = metrology.fn_check_plan_status_id('DONE')
            WHERE id = p_check_plan_id;
          END IF;

          RETURN v_event_id;
        END;
        $$;

        -- ===== Decommission / replace instrument =====
        CREATE OR REPLACE FUNCTION metrology.fn_decommission_instrument(
          p_instrument_id uuid,
          p_reason text,
          p_replaced_by_instrument_id uuid DEFAULT NULL
        )
        RETURNS void
        LANGUAGE plpgsql
        AS $$
        DECLARE
          v_status uuid;
        BEGIN
          IF p_replaced_by_instrument_id IS NULL THEN
            v_status := metrology.fn_instrument_status_id('DECOMMISSIONED');
          ELSE
            v_status := metrology.fn_instrument_status_id('REPLACED');
          END IF;

          IF v_status IS NULL THEN
            RAISE EXCEPTION 'Instrument status not seeded';
          END IF;

          UPDATE metrology.instrument
            SET status_id = v_status,
                decommissioned_at = now(),
                decommission_reason = p_reason,
                replaced_by_instrument_id = p_replaced_by_instrument_id
          WHERE id = p_instrument_id;
        END;
        $$;

        -- ===== Generate plans in a date range (based on next_due) =====
        CREATE OR REPLACE FUNCTION metrology.fn_generate_check_plan(
          p_from date,
          p_to date
        )
        RETURNS integer
        LANGUAGE plpgsql
        AS $$
        DECLARE
          v_planned_status uuid;
          v_active_status uuid;
          v_inserted integer;
        BEGIN
          IF p_from IS NULL OR p_to IS NULL OR p_to < p_from THEN
            RAISE EXCEPTION 'Invalid range';
          END IF;

          v_planned_status := metrology.fn_check_plan_status_id('PLANNED');
          v_active_status := metrology.fn_instrument_status_id('ACTIVE');

          IF v_planned_status IS NULL THEN
            RAISE EXCEPTION 'Plan status not seeded';
          END IF;

          WITH candidates AS (
            SELECT
              v.instrument_id,
              v.check_type_id,
              v.next_due_date AS due_date
            FROM metrology.v_instrument_check_next_due v
            JOIN metrology.instrument i ON i.id = v.instrument_id
            WHERE v.next_due_date IS NOT NULL
              AND v.next_due_date BETWEEN p_from AND p_to
              AND i.status_id = v_active_status
          ),
          ins AS (
            INSERT INTO metrology.check_plan(
              instrument_id, check_type_id, due_date, status_id
            )
            SELECT c.instrument_id, c.check_type_id, c.due_date, v_planned_status
            FROM candidates c
            ON CONFLICT ON CONSTRAINT uq_check_plan DO NOTHING
            RETURNING 1
          )
          SELECT count(*) INTO v_inserted FROM ins;

          RETURN v_inserted;
        END;
        $$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP FUNCTION IF EXISTS metrology.fn_generate_check_plan(date, date);
        DROP FUNCTION IF EXISTS metrology.fn_decommission_instrument(uuid, text, uuid);
        DROP FUNCTION IF EXISTS metrology.fn_register_check_event(uuid, uuid, date, text, uuid, uuid, uuid, text, text, uuid[]);

        DROP FUNCTION IF EXISTS metrology.fn_check_plan_status_id(text);
        DROP FUNCTION IF EXISTS metrology.fn_instrument_status_id(text);
        DROP FUNCTION IF EXISTS metrology.fn_check_result_status_id(text);

        ALTER TABLE metrology.check_plan DROP CONSTRAINT IF EXISTS uq_check_plan;
        """
    )


