-- REFRESH MATERIALIZED VIEW CONCURRENTLY нельзя выполнять внутри явного transaction block.
-- В psql просто выполните эти команды отдельно (без BEGIN/COMMIT вокруг).

REFRESH MATERIALIZED VIEW CONCURRENTLY metrology.mv_instruments_due_30d;
REFRESH MATERIALIZED VIEW CONCURRENTLY metrology.mv_instruments_overdue;


