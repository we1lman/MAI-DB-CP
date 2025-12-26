## ИС учёта поверок/калибровок (DB-first, PostgreSQL 16 + FastAPI)

Проект ориентирован на **качество SQL и моделирования БД**: 3НФ, ограничения, триггеры аудита, VIEW/MATERIALIZED VIEW, индексы, функции/процедуры, демонстрация изоляций/конкуренции.

### Состав
- `docs/` — словарь полей и ER-модель
- `migrations/` — Alembic миграции (DDL/триггеры/VIEW/MV/индексы в явном SQL)
- `db/seed/` — seed-данные
- `db/scripts/` — демонстрационные SQL-сценарии (изоляции, отчёты)
- `app/` — FastAPI (Swagger)

### Запуск (Docker)
1) Скопируйте `env.template` → `.env` (локально) и задайте пароли.  
   Примечание: в этом окружении файл `.env` может не храниться в репозитории — это нормально и безопаснее.
2) Поднимите сервисы:

```bash
docker compose up -d --build
```

3) Выполните миграции:

```bash
docker compose exec api alembic upgrade head
```

Swagger: `http://localhost:8000/docs`


