-- Демонстрация конкуренции без блокировок через SKIP LOCKED
-- Идея: несколько воркеров «разбирают» кандидатов на планирование, не мешая друг другу.
--
-- Примечание: это учебный пример. В проде обычно делается отдельная очередь/таблица задач.

-- Пример: создадим временную очередь кандидатов (на время демонстрации)
-- и будем забирать порции.

-- ========== INIT (один раз) ==========
-- DROP TABLE IF EXISTS metrology.plan_queue;
-- CREATE TABLE metrology.plan_queue(
--   instrument_id uuid NOT NULL,
--   check_type_id uuid NOT NULL,
--   due_date date NOT NULL,
--   PRIMARY KEY (instrument_id, check_type_id, due_date)
-- );
--
-- INSERT INTO metrology.plan_queue(instrument_id, check_type_id, due_date)
-- SELECT instrument_id, check_type_id, next_due_date
-- FROM metrology.v_instrument_check_next_due
-- WHERE next_due_date BETWEEN current_date AND current_date + 30;

-- ========== WORKER (несколько сессий параллельно) ==========
-- BEGIN;
-- WITH batch AS (
--   SELECT instrument_id, check_type_id, due_date
--   FROM metrology.plan_queue
--   ORDER BY due_date
--   FOR UPDATE SKIP LOCKED
--   LIMIT 50
-- ),
-- ins AS (
--   INSERT INTO metrology.check_plan(instrument_id, check_type_id, due_date, status_id)
--   SELECT b.instrument_id, b.check_type_id, b.due_date,
--          metrology.fn_check_plan_status_id('PLANNED')
--   FROM batch b
--   ON CONFLICT ON CONSTRAINT uq_check_plan DO NOTHING
--   RETURNING 1
-- )
-- DELETE FROM metrology.plan_queue q
-- USING batch b
-- WHERE q.instrument_id=b.instrument_id AND q.check_type_id=b.check_type_id AND q.due_date=b.due_date;
-- COMMIT;


