"""triggers: audit log, status history, consistency, next_due_date

Revision ID: 0002_triggers_audit_status_next_due
Revises: 0001_init_schema_tables
Create Date: 2025-12-26
"""

from __future__ import annotations

from alembic import op

revision = "0002_triggers_audit_status_next_due"
down_revision = "0001_init_schema_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        -- ===== Audit trigger =====
        CREATE OR REPLACE FUNCTION metrology.trg_audit_row()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          v_row_id uuid;
        BEGIN
          IF TG_OP = 'DELETE' THEN
            v_row_id := OLD.id;
          ELSE
            v_row_id := NEW.id;
          END IF;

          INSERT INTO metrology.audit_log(action, table_name, row_id, old_row, new_row)
          VALUES (
            TG_OP,
            TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME,
            v_row_id,
            CASE WHEN TG_OP IN ('UPDATE','DELETE') THEN to_jsonb(OLD) ELSE NULL END,
            CASE WHEN TG_OP IN ('INSERT','UPDATE') THEN to_jsonb(NEW) ELSE NULL END
          );

          IF TG_OP = 'DELETE' THEN
            RETURN OLD;
          END IF;
          RETURN NEW;
        END;
        $$;

        -- Attach audit triggers (idempotent: drop/recreate)
        DROP TRIGGER IF EXISTS trg_audit_instrument ON metrology.instrument;
        CREATE TRIGGER trg_audit_instrument
          AFTER INSERT OR UPDATE OR DELETE ON metrology.instrument
          FOR EACH ROW EXECUTE FUNCTION metrology.trg_audit_row();

        DROP TRIGGER IF EXISTS trg_audit_check_plan ON metrology.check_plan;
        CREATE TRIGGER trg_audit_check_plan
          AFTER INSERT OR UPDATE OR DELETE ON metrology.check_plan
          FOR EACH ROW EXECUTE FUNCTION metrology.trg_audit_row();

        DROP TRIGGER IF EXISTS trg_audit_check_event ON metrology.check_event;
        CREATE TRIGGER trg_audit_check_event
          AFTER INSERT OR UPDATE OR DELETE ON metrology.check_event
          FOR EACH ROW EXECUTE FUNCTION metrology.trg_audit_row();

        DROP TRIGGER IF EXISTS trg_audit_check_requirement ON metrology.check_requirement;
        CREATE TRIGGER trg_audit_check_requirement
          AFTER INSERT OR UPDATE OR DELETE ON metrology.check_requirement
          FOR EACH ROW EXECUTE FUNCTION metrology.trg_audit_row();

        DROP TRIGGER IF EXISTS trg_audit_document ON metrology.document;
        CREATE TRIGGER trg_audit_document
          AFTER INSERT OR UPDATE OR DELETE ON metrology.document
          FOR EACH ROW EXECUTE FUNCTION metrology.trg_audit_row();

        -- ===== Instrument status history =====
        CREATE UNIQUE INDEX IF NOT EXISTS uq_ish_one_open
          ON metrology.instrument_status_history(instrument_id)
          WHERE valid_to IS NULL;

        CREATE OR REPLACE FUNCTION metrology.trg_instrument_status_history()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          IF TG_OP = 'INSERT' THEN
            INSERT INTO metrology.instrument_status_history(instrument_id, status_id, valid_from, reason)
            VALUES (NEW.id, NEW.status_id, now(), 'initial');
            RETURN NEW;
          END IF;

          IF TG_OP = 'UPDATE' AND NEW.status_id IS DISTINCT FROM OLD.status_id THEN
            UPDATE metrology.instrument_status_history
              SET valid_to = now()
            WHERE instrument_id = NEW.id
              AND valid_to IS NULL;

            INSERT INTO metrology.instrument_status_history(instrument_id, status_id, valid_from, reason)
            VALUES (NEW.id, NEW.status_id, now(), COALESCE(NEW.decommission_reason, 'status change'));

            RETURN NEW;
          END IF;

          RETURN NEW;
        END;
        $$;

        DROP TRIGGER IF EXISTS trg_instrument_status_history_ins ON metrology.instrument;
        CREATE TRIGGER trg_instrument_status_history_ins
          AFTER INSERT ON metrology.instrument
          FOR EACH ROW EXECUTE FUNCTION metrology.trg_instrument_status_history();

        DROP TRIGGER IF EXISTS trg_instrument_status_history_upd ON metrology.instrument;
        CREATE TRIGGER trg_instrument_status_history_upd
          AFTER UPDATE OF status_id ON metrology.instrument
          FOR EACH ROW EXECUTE FUNCTION metrology.trg_instrument_status_history();

        -- ===== Consistency: instrument.org_unit must match location.org_unit =====
        CREATE OR REPLACE FUNCTION metrology.trg_assert_instrument_location_org_unit()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          v_loc_ou uuid;
        BEGIN
          SELECT org_unit_id INTO v_loc_ou
          FROM metrology.location
          WHERE id = NEW.location_id;

          IF v_loc_ou IS NULL THEN
            RAISE EXCEPTION 'Invalid location_id=%', NEW.location_id;
          END IF;

          IF v_loc_ou <> NEW.org_unit_id THEN
            RAISE EXCEPTION 'Location org_unit_id (%) != instrument.org_unit_id (%)', v_loc_ou, NEW.org_unit_id;
          END IF;

          RETURN NEW;
        END;
        $$;

        DROP TRIGGER IF EXISTS trg_instrument_location_org_unit ON metrology.instrument;
        CREATE TRIGGER trg_instrument_location_org_unit
          BEFORE INSERT OR UPDATE OF org_unit_id, location_id ON metrology.instrument
          FOR EACH ROW EXECUTE FUNCTION metrology.trg_assert_instrument_location_org_unit();

        -- ===== Consistency: check_event with plan must match instrument + check_type =====
        CREATE OR REPLACE FUNCTION metrology.trg_assert_event_matches_plan()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          v_plan_instrument uuid;
          v_plan_check_type uuid;
        BEGIN
          IF NEW.check_plan_id IS NULL THEN
            RETURN NEW;
          END IF;

          SELECT instrument_id, check_type_id
            INTO v_plan_instrument, v_plan_check_type
          FROM metrology.check_plan
          WHERE id = NEW.check_plan_id;

          IF v_plan_instrument IS NULL THEN
            RAISE EXCEPTION 'Invalid check_plan_id=%', NEW.check_plan_id;
          END IF;

          IF v_plan_instrument <> NEW.instrument_id OR v_plan_check_type <> NEW.check_type_id THEN
            RAISE EXCEPTION 'check_event(instrument_id,check_type_id) must match check_plan';
          END IF;

          RETURN NEW;
        END;
        $$;

        DROP TRIGGER IF EXISTS trg_event_matches_plan ON metrology.check_event;
        CREATE TRIGGER trg_event_matches_plan
          BEFORE INSERT OR UPDATE OF check_plan_id, instrument_id, check_type_id ON metrology.check_event
          FOR EACH ROW EXECUTE FUNCTION metrology.trg_assert_event_matches_plan();

        -- ===== next_due_date computation =====
        CREATE OR REPLACE FUNCTION metrology.fn_compute_next_due_date(
          p_instrument_id uuid,
          p_check_type_id uuid,
          p_check_date date
        )
        RETURNS date
        LANGUAGE plpgsql
        AS $$
        DECLARE
          v_model_id uuid;
          v_interval_months integer;
        BEGIN
          SELECT instrument_model_id INTO v_model_id
          FROM metrology.instrument
          WHERE id = p_instrument_id;

          IF v_model_id IS NULL THEN
            RAISE EXCEPTION 'Invalid instrument_id=%', p_instrument_id;
          END IF;

          SELECT interval_months INTO v_interval_months
          FROM metrology.check_requirement
          WHERE instrument_model_id = v_model_id
            AND check_type_id = p_check_type_id;

          IF v_interval_months IS NULL THEN
            RETURN NULL;
          END IF;

          RETURN (p_check_date + make_interval(months => v_interval_months))::date;
        END;
        $$;

        CREATE OR REPLACE FUNCTION metrology.trg_check_event_set_next_due()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
          v_is_success boolean;
        BEGIN
          SELECT is_success INTO v_is_success
          FROM metrology.check_result_status
          WHERE id = NEW.result_status_id;

          IF v_is_success IS TRUE THEN
            NEW.next_due_date := metrology.fn_compute_next_due_date(NEW.instrument_id, NEW.check_type_id, NEW.check_date);
          ELSE
            NEW.next_due_date := NULL;
          END IF;

          RETURN NEW;
        END;
        $$;

        DROP TRIGGER IF EXISTS trg_check_event_set_next_due ON metrology.check_event;
        CREATE TRIGGER trg_check_event_set_next_due
          BEFORE INSERT OR UPDATE OF result_status_id, check_date, instrument_id, check_type_id ON metrology.check_event
          FOR EACH ROW EXECUTE FUNCTION metrology.trg_check_event_set_next_due();
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TRIGGER IF EXISTS trg_check_event_set_next_due ON metrology.check_event;
        DROP FUNCTION IF EXISTS metrology.trg_check_event_set_next_due();
        DROP FUNCTION IF EXISTS metrology.fn_compute_next_due_date(uuid, uuid, date);

        DROP TRIGGER IF EXISTS trg_event_matches_plan ON metrology.check_event;
        DROP FUNCTION IF EXISTS metrology.trg_assert_event_matches_plan();

        DROP TRIGGER IF EXISTS trg_instrument_location_org_unit ON metrology.instrument;
        DROP FUNCTION IF EXISTS metrology.trg_assert_instrument_location_org_unit();

        DROP TRIGGER IF EXISTS trg_instrument_status_history_ins ON metrology.instrument;
        DROP TRIGGER IF EXISTS trg_instrument_status_history_upd ON metrology.instrument;
        DROP FUNCTION IF EXISTS metrology.trg_instrument_status_history();
        DROP INDEX IF EXISTS metrology.uq_ish_one_open;

        DROP TRIGGER IF EXISTS trg_audit_instrument ON metrology.instrument;
        DROP TRIGGER IF EXISTS trg_audit_check_plan ON metrology.check_plan;
        DROP TRIGGER IF EXISTS trg_audit_check_event ON metrology.check_event;
        DROP TRIGGER IF EXISTS trg_audit_check_requirement ON metrology.check_requirement;
        DROP TRIGGER IF EXISTS trg_audit_document ON metrology.document;
        DROP FUNCTION IF EXISTS metrology.trg_audit_row();
        """
    )


