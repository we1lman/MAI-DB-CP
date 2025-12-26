-- Демонстрация изоляции SERIALIZABLE (две сессии A/B)
-- Цель: показать, что два параллельных генератора планов могут конфликтовать, и один получит serialization_failure.
--
-- Подготовка: выполните миграции и seed-данные (см. db/seed), чтобы были инструменты и next_due_date.
--
-- ВАЖНО: открывайте в ДВУХ psql-сессиях.

-- ========== SESSION A ==========
-- BEGIN;
-- SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
-- SELECT metrology.fn_generate_check_plan(current_date, current_date + 30);
-- -- не коммитьте сразу, подождите, пока B сделает то же самое
-- COMMIT;

-- ========== SESSION B ==========
-- BEGIN;
-- SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
-- SELECT metrology.fn_generate_check_plan(current_date, current_date + 30);
-- COMMIT;

-- Ожидаемое поведение:
-- - Одна из транзакций может завершиться ошибкой:
--   ERROR: could not serialize access due to read/write dependencies among transactions
-- - Правильная стратегия приложения/батча: повторить транзакцию.


