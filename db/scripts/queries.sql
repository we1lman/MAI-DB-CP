-- Типовые отчёты/запросы (для защиты проекта)

-- 1) Приборы с истекающим сроком (30 дней) — из MV
SELECT inventory_no, check_type_code, last_check_date, next_due_date, days_to_due
FROM metrology.mv_instruments_due_30d
ORDER BY next_due_date, inventory_no;

-- 2) Просроченные — из MV
SELECT inventory_no, check_type_code, last_check_date, next_due_date, days_to_due
FROM metrology.mv_instruments_overdue
ORDER BY next_due_date, inventory_no;

-- 3) Актуальная следующая дата по прибору (минимальная по типам)
SELECT inventory_no, next_due_date, days_to_due
FROM metrology.v_instrument_next_due
ORDER BY next_due_date NULLS LAST, inventory_no;

-- 4) По лабораториям (факт)
SELECT
  l.code,
  l.name,
  count(*) AS events_total,
  sum(CASE WHEN rs.is_success THEN 1 ELSE 0 END) AS events_passed
FROM metrology.check_event ce
JOIN metrology.lab l ON l.id = ce.lab_id
JOIN metrology.check_result_status rs ON rs.id = ce.result_status_id
GROUP BY l.code, l.name
ORDER BY events_total DESC;

-- 5) По подразделениям (кол-во приборов)
SELECT ou.code, ou.name, count(i.id) AS instruments_total
FROM metrology.org_unit ou
LEFT JOIN metrology.instrument i ON i.org_unit_id = ou.id
GROUP BY ou.code, ou.name
ORDER BY ou.code;

-- 6) Аудит изменений по прибору
-- Подставьте row_id конкретного instrument.id
-- SELECT * FROM metrology.audit_log WHERE table_name='metrology.instrument' AND row_id='...' ORDER BY at DESC;


