"""init schema metrology + core tables (3NF)

Revision ID: 0001_init_schema_tables
Revises: None
Create Date: 2025-12-26
"""

from __future__ import annotations

from alembic import op

revision = "0001_init_schema_tables"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        -- Extensions
        CREATE EXTENSION IF NOT EXISTS pgcrypto;

        -- Dedicated schema
        CREATE SCHEMA IF NOT EXISTS metrology;

        -- ===== Reference tables (lookups) =====
        CREATE TABLE IF NOT EXISTS metrology.instrument_status (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          code text NOT NULL UNIQUE,
          name text NOT NULL
        );

        CREATE TABLE IF NOT EXISTS metrology.check_result_status (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          code text NOT NULL UNIQUE,
          name text NOT NULL,
          is_success boolean NOT NULL DEFAULT false
        );

        CREATE TABLE IF NOT EXISTS metrology.check_kind (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          code text NOT NULL UNIQUE,
          name text NOT NULL
        );

        CREATE TABLE IF NOT EXISTS metrology.check_plan_status (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          code text NOT NULL UNIQUE,
          name text NOT NULL
        );

        CREATE TABLE IF NOT EXISTS metrology.document_type (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          code text NOT NULL UNIQUE,
          name text NOT NULL
        );

        -- ===== Directories =====
        CREATE TABLE IF NOT EXISTS metrology.org_unit (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          code text NOT NULL UNIQUE,
          name text NOT NULL,
          parent_id uuid NULL,
          CONSTRAINT fk_org_unit_parent
            FOREIGN KEY (parent_id) REFERENCES metrology.org_unit(id) ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS metrology.location (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          org_unit_id uuid NOT NULL,
          code text NOT NULL,
          name text NOT NULL,
          CONSTRAINT fk_location_org_unit
            FOREIGN KEY (org_unit_id) REFERENCES metrology.org_unit(id) ON DELETE RESTRICT,
          CONSTRAINT uq_location_org_unit_code UNIQUE (org_unit_id, code)
        );

        CREATE TABLE IF NOT EXISTS metrology.lab (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          code text NOT NULL UNIQUE,
          name text NOT NULL,
          accreditation_no text NULL,
          contacts jsonb NULL
        );

        CREATE TABLE IF NOT EXISTS metrology.specialist (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          lab_id uuid NULL,
          full_name text NOT NULL,
          position text NULL,
          email text NULL,
          phone text NULL,
          CONSTRAINT fk_specialist_lab
            FOREIGN KEY (lab_id) REFERENCES metrology.lab(id) ON DELETE RESTRICT
        );

        -- ===== Nomenclature =====
        CREATE TABLE IF NOT EXISTS metrology.instrument_type (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          code text NOT NULL UNIQUE,
          name text NOT NULL
        );

        CREATE TABLE IF NOT EXISTS metrology.instrument_model (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          instrument_type_id uuid NOT NULL,
          manufacturer text NOT NULL,
          model_name text NOT NULL,
          description text NULL,
          CONSTRAINT fk_instrument_model_type
            FOREIGN KEY (instrument_type_id) REFERENCES metrology.instrument_type(id) ON DELETE RESTRICT,
          CONSTRAINT uq_model UNIQUE (instrument_type_id, manufacturer, model_name)
        );

        CREATE TABLE IF NOT EXISTS metrology.instrument (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          instrument_model_id uuid NOT NULL,
          inventory_no text NOT NULL,
          serial_no text NULL,

          -- Structured measurement range / error (can be extended later)
          range_min numeric NULL,
          range_max numeric NULL,
          range_unit text NULL,
          error_limit numeric NULL,
          error_unit text NULL,
          accuracy_class text NULL,

          org_unit_id uuid NOT NULL,
          location_id uuid NOT NULL,
          installed_at timestamptz NULL,

          status_id uuid NOT NULL,
          replaced_by_instrument_id uuid NULL,

          decommissioned_at timestamptz NULL,
          decommission_reason text NULL,

          CONSTRAINT fk_instrument_model
            FOREIGN KEY (instrument_model_id) REFERENCES metrology.instrument_model(id) ON DELETE RESTRICT,
          CONSTRAINT fk_instrument_org_unit
            FOREIGN KEY (org_unit_id) REFERENCES metrology.org_unit(id) ON DELETE RESTRICT,
          CONSTRAINT fk_instrument_location
            FOREIGN KEY (location_id) REFERENCES metrology.location(id) ON DELETE RESTRICT,
          CONSTRAINT fk_instrument_status
            FOREIGN KEY (status_id) REFERENCES metrology.instrument_status(id) ON DELETE RESTRICT,
          CONSTRAINT fk_instrument_replaced_by
            FOREIGN KEY (replaced_by_instrument_id) REFERENCES metrology.instrument(id) ON DELETE RESTRICT,

          CONSTRAINT uq_instrument_inventory UNIQUE (inventory_no),
          CONSTRAINT ck_range_order CHECK (
            range_min IS NULL OR range_max IS NULL OR range_min <= range_max
          )
        );

        -- ===== Metrology operations =====
        CREATE TABLE IF NOT EXISTS metrology.check_type (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          code text NOT NULL UNIQUE,
          name text NOT NULL,
          check_kind_id uuid NOT NULL,
          CONSTRAINT fk_check_type_kind
            FOREIGN KEY (check_kind_id) REFERENCES metrology.check_kind(id) ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS metrology.check_requirement (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          instrument_model_id uuid NOT NULL,
          check_type_id uuid NOT NULL,
          interval_months integer NOT NULL,
          grace_days integer NOT NULL DEFAULT 0,
          is_mandatory boolean NOT NULL DEFAULT true,
          notes text NULL,
          CONSTRAINT fk_req_model
            FOREIGN KEY (instrument_model_id) REFERENCES metrology.instrument_model(id) ON DELETE RESTRICT,
          CONSTRAINT fk_req_check_type
            FOREIGN KEY (check_type_id) REFERENCES metrology.check_type(id) ON DELETE RESTRICT,
          CONSTRAINT uq_req UNIQUE (instrument_model_id, check_type_id),
          CONSTRAINT ck_interval_months CHECK (interval_months > 0),
          CONSTRAINT ck_grace_days CHECK (grace_days >= 0)
        );

        -- ===== Plan / Fact =====
        CREATE TABLE IF NOT EXISTS metrology.check_plan (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          instrument_id uuid NOT NULL,
          check_type_id uuid NOT NULL,
          due_date date NOT NULL,
          planned_lab_id uuid NULL,
          planned_specialist_id uuid NULL,
          status_id uuid NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          notes text NULL,
          CONSTRAINT fk_plan_instrument
            FOREIGN KEY (instrument_id) REFERENCES metrology.instrument(id) ON DELETE RESTRICT,
          CONSTRAINT fk_plan_check_type
            FOREIGN KEY (check_type_id) REFERENCES metrology.check_type(id) ON DELETE RESTRICT,
          CONSTRAINT fk_plan_lab
            FOREIGN KEY (planned_lab_id) REFERENCES metrology.lab(id) ON DELETE RESTRICT,
          CONSTRAINT fk_plan_specialist
            FOREIGN KEY (planned_specialist_id) REFERENCES metrology.specialist(id) ON DELETE RESTRICT,
          CONSTRAINT fk_plan_status
            FOREIGN KEY (status_id) REFERENCES metrology.check_plan_status(id) ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS metrology.check_event (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          instrument_id uuid NOT NULL,
          check_plan_id uuid NULL,
          check_type_id uuid NOT NULL,
          lab_id uuid NOT NULL,
          specialist_id uuid NULL,
          check_date date NOT NULL,
          result_status_id uuid NOT NULL,
          protocol_no text NULL,
          next_due_date date NULL,
          notes text NULL,
          created_at timestamptz NOT NULL DEFAULT now(),

          CONSTRAINT fk_event_instrument
            FOREIGN KEY (instrument_id) REFERENCES metrology.instrument(id) ON DELETE RESTRICT,
          CONSTRAINT fk_event_plan
            FOREIGN KEY (check_plan_id) REFERENCES metrology.check_plan(id) ON DELETE RESTRICT,
          CONSTRAINT fk_event_check_type
            FOREIGN KEY (check_type_id) REFERENCES metrology.check_type(id) ON DELETE RESTRICT,
          CONSTRAINT fk_event_lab
            FOREIGN KEY (lab_id) REFERENCES metrology.lab(id) ON DELETE RESTRICT,
          CONSTRAINT fk_event_specialist
            FOREIGN KEY (specialist_id) REFERENCES metrology.specialist(id) ON DELETE RESTRICT,
          CONSTRAINT fk_event_result
            FOREIGN KEY (result_status_id) REFERENCES metrology.check_result_status(id) ON DELETE RESTRICT,

          CONSTRAINT uq_event_plan UNIQUE (check_plan_id),
          CONSTRAINT ck_next_due CHECK (next_due_date IS NULL OR next_due_date >= check_date)
        );

        -- ===== Documents =====
        CREATE TABLE IF NOT EXISTS metrology.document (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          document_type_id uuid NOT NULL,
          title text NOT NULL,
          storage_ref text NOT NULL,
          sha256 text NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT fk_document_type
            FOREIGN KEY (document_type_id) REFERENCES metrology.document_type(id) ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS metrology.check_event_document (
          check_event_id uuid NOT NULL,
          document_id uuid NOT NULL,
          PRIMARY KEY (check_event_id, document_id),
          CONSTRAINT fk_ced_event
            FOREIGN KEY (check_event_id) REFERENCES metrology.check_event(id) ON DELETE RESTRICT,
          CONSTRAINT fk_ced_document
            FOREIGN KEY (document_id) REFERENCES metrology.document(id) ON DELETE RESTRICT
        );

        -- ===== Status history =====
        CREATE TABLE IF NOT EXISTS metrology.instrument_status_history (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          instrument_id uuid NOT NULL,
          status_id uuid NOT NULL,
          valid_from timestamptz NOT NULL DEFAULT now(),
          valid_to timestamptz NULL,
          reason text NULL,
          CONSTRAINT fk_ish_instrument
            FOREIGN KEY (instrument_id) REFERENCES metrology.instrument(id) ON DELETE RESTRICT,
          CONSTRAINT fk_ish_status
            FOREIGN KEY (status_id) REFERENCES metrology.instrument_status(id) ON DELETE RESTRICT,
          CONSTRAINT ck_ish_period CHECK (valid_to IS NULL OR valid_to >= valid_from)
        );

        -- ===== Audit =====
        CREATE TABLE IF NOT EXISTS metrology.audit_log (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          at timestamptz NOT NULL DEFAULT now(),
          db_user text NOT NULL DEFAULT current_user,
          action text NOT NULL,
          table_name text NOT NULL,
          row_id uuid NULL,
          old_row jsonb NULL,
          new_row jsonb NULL
        );

        -- ===== Minimal seed for lookups (idempotent) =====
        INSERT INTO metrology.instrument_status(code, name)
        VALUES
          ('ACTIVE', 'В эксплуатации'),
          ('IN_REPAIR', 'В ремонте'),
          ('DECOMMISSIONED', 'Выведен из эксплуатации'),
          ('REPLACED', 'Заменён')
        ON CONFLICT (code) DO NOTHING;

        INSERT INTO metrology.check_result_status(code, name, is_success)
        VALUES
          ('PASSED', 'Годен', true),
          ('FAILED', 'Не годен', false),
          ('CANCELED', 'Отменено', false)
        ON CONFLICT (code) DO NOTHING;

        INSERT INTO metrology.check_kind(code, name)
        VALUES
          ('VERIFICATION', 'Поверка'),
          ('CALIBRATION', 'Калибровка')
        ON CONFLICT (code) DO NOTHING;

        INSERT INTO metrology.check_plan_status(code, name)
        VALUES
          ('PLANNED', 'Запланировано'),
          ('DONE', 'Выполнено'),
          ('CANCELED', 'Отменено')
        ON CONFLICT (code) DO NOTHING;

        INSERT INTO metrology.document_type(code, name)
        VALUES
          ('PROTOCOL', 'Протокол'),
          ('CERTIFICATE', 'Свидетельство'),
          ('OTHER', 'Прочее')
        ON CONFLICT (code) DO NOTHING;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP SCHEMA IF EXISTS metrology CASCADE;
        """
    )


