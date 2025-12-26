-- Базовые справочники и номенклатура для демо
-- Выполнять после миграций (alembic upgrade head).

-- org_unit
WITH ou AS (
  INSERT INTO metrology.org_unit(code, name)
  VALUES
    ('PROD', 'Производство'),
    ('QC', 'ОТК')
  ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
  RETURNING id, code
)
SELECT * FROM ou;

-- location
WITH prod AS (
  SELECT id FROM metrology.org_unit WHERE code='PROD'
),
ins AS (
  INSERT INTO metrology.location(org_unit_id, code, name)
  SELECT prod.id, v.code, v.name
  FROM prod
  CROSS JOIN (VALUES
    ('LINE_1','Линия 1'),
    ('LINE_2','Линия 2')
  ) AS v(code, name)
  ON CONFLICT (org_unit_id, code) DO UPDATE SET name = EXCLUDED.name
  RETURNING *
)
SELECT * FROM ins;

-- labs + specialists
WITH lab AS (
  INSERT INTO metrology.lab(code, name, accreditation_no, contacts)
  VALUES ('LAB_A', 'Лаборатория А', 'ACC-001', '{"phone":"+7-000-000-00-00"}'::jsonb)
  ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
  RETURNING id
),
spec AS (
  INSERT INTO metrology.specialist(lab_id, full_name, position, email)
  SELECT lab.id, 'Иванов И.И.', 'Инженер-метролог', 'ivanov@example.local'
  FROM lab
  ON CONFLICT DO NOTHING
  RETURNING *
)
SELECT * FROM spec;

-- instrument type/model
WITH it AS (
  INSERT INTO metrology.instrument_type(code, name)
  VALUES ('PRESSURE', 'Датчик давления')
  ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
  RETURNING id
),
im AS (
  INSERT INTO metrology.instrument_model(instrument_type_id, manufacturer, model_name, description)
  SELECT it.id, 'ACME', 'P-100', 'Датчик давления P-100'
  FROM it
  ON CONFLICT (instrument_type_id, manufacturer, model_name) DO UPDATE SET description = EXCLUDED.description
  RETURNING id
)
SELECT * FROM im;

-- check types (verification/calibration)
WITH kind_v AS (SELECT id FROM metrology.check_kind WHERE code='VERIFICATION'),
kind_c AS (SELECT id FROM metrology.check_kind WHERE code='CALIBRATION'),
ins AS (
  INSERT INTO metrology.check_type(code, name, check_kind_id)
  VALUES
    ('VERIF', 'Поверка', (SELECT id FROM kind_v)),
    ('CALIB', 'Калибровка', (SELECT id FROM kind_c))
  ON CONFLICT (code) DO NOTHING
  RETURNING *
)
SELECT * FROM ins;

-- requirements: for model P-100 (VERIF every 12 months)
WITH model AS (
  SELECT id FROM metrology.instrument_model WHERE manufacturer='ACME' AND model_name='P-100'
),
ct AS (
  SELECT id FROM metrology.check_type WHERE code='VERIF'
),
ins AS (
  INSERT INTO metrology.check_requirement(instrument_model_id, check_type_id, interval_months, grace_days, is_mandatory, notes)
  SELECT model.id, ct.id, 12, 0, true, 'Ежегодная поверка'
  FROM model, ct
  ON CONFLICT (instrument_model_id, check_type_id) DO UPDATE
    SET interval_months=EXCLUDED.interval_months, grace_days=EXCLUDED.grace_days, notes=EXCLUDED.notes
  RETURNING *
)
SELECT * FROM ins;


