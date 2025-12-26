-- Демо-экземпляры приборов + события поверок (для MV overdue / 30d)

-- Resolve needed IDs
WITH
  model AS (SELECT id FROM metrology.instrument_model WHERE manufacturer='ACME' AND model_name='P-100'),
  ou AS (SELECT id FROM metrology.org_unit WHERE code='PROD'),
  loc1 AS (SELECT id FROM metrology.location WHERE code='LINE_1' AND org_unit_id=(SELECT id FROM ou)),
  loc2 AS (SELECT id FROM metrology.location WHERE code='LINE_2' AND org_unit_id=(SELECT id FROM ou)),
  st_active AS (SELECT id FROM metrology.instrument_status WHERE code='ACTIVE'),
  lab AS (SELECT id FROM metrology.lab WHERE code='LAB_A'),
  spec AS (SELECT id FROM metrology.specialist WHERE full_name='Иванов И.И.' LIMIT 1),
  ct AS (SELECT id FROM metrology.check_type WHERE code='VERIF')
INSERT INTO metrology.instrument(
  instrument_model_id, inventory_no, serial_no,
  range_min, range_max, range_unit,
  error_limit, error_unit, accuracy_class,
  org_unit_id, location_id, installed_at, status_id
)
SELECT
  (SELECT id FROM model),
  v.inventory_no,
  v.serial_no,
  v.range_min, v.range_max, v.range_unit,
  v.error_limit, v.error_unit, v.accuracy_class,
  (SELECT id FROM ou),
  v.location_id,
  now(),
  (SELECT id FROM st_active)
FROM (
  VALUES
    ('INV-0001','SN-0001', 0.0, 10.0, 'bar', 0.1, 'bar', '0.5', (SELECT id FROM loc1)),
    ('INV-0002','SN-0002', 0.0, 10.0, 'bar', 0.1, 'bar', '0.5', (SELECT id FROM loc2))
) AS v(inventory_no, serial_no, range_min, range_max, range_unit, error_limit, error_unit, accuracy_class, location_id)
ON CONFLICT (inventory_no) DO NOTHING;

-- Create 2 events: one due in ~1 month, one overdue
WITH
  i1 AS (SELECT id FROM metrology.instrument WHERE inventory_no='INV-0001'),
  i2 AS (SELECT id FROM metrology.instrument WHERE inventory_no='INV-0002'),
  ct AS (SELECT id FROM metrology.check_type WHERE code='VERIF'),
  lab AS (SELECT id FROM metrology.lab WHERE code='LAB_A'),
  spec AS (SELECT id FROM metrology.specialist WHERE full_name='Иванов И.И.' LIMIT 1),
  rs_pass AS (SELECT id FROM metrology.check_result_status WHERE code='PASSED')
INSERT INTO metrology.check_event(
  instrument_id, check_type_id, lab_id, specialist_id,
  check_date, result_status_id, protocol_no, notes
)
VALUES
  ((SELECT id FROM i1), (SELECT id FROM ct), (SELECT id FROM lab), (SELECT id FROM spec),
   (current_date - interval '11 months')::date, (SELECT id FROM rs_pass), 'PR-001', 'Демо: срок скоро'),
  ((SELECT id FROM i2), (SELECT id FROM ct), (SELECT id FROM lab), (SELECT id FROM spec),
   (current_date - interval '14 months')::date, (SELECT id FROM rs_pass), 'PR-002', 'Демо: просрочено')
ON CONFLICT DO NOTHING;

-- Refresh non-concurrent MV for immediate reporting
CALL metrology.sp_refresh_due_mviews();


