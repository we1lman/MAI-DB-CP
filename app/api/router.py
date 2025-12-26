from __future__ import annotations

from datetime import date, datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.db import get_conn
from app.schemas import (
    AuditRowOut,
    CheckEventOut,
    CheckPlanCreate,
    CheckPlanOut,
    CheckPlanUpdate,
    CheckRequirementCreate,
    CheckRequirementOut,
    CheckRequirementUpdate,
    CheckTypeCreate,
    CheckTypeOut,
    CheckTypeUpdate,
    DecommissionInstrumentIn,
    DocumentCreate,
    DocumentOut,
    DocumentUpdate,
    GeneratePlansIn,
    GeneratePlansOut,
    InstrumentCreate,
    InstrumentModelCreate,
    InstrumentModelOut,
    InstrumentModelUpdate,
    InstrumentOut,
    InstrumentUpdate,
    InstrumentTypeCreate,
    InstrumentTypeOut,
    InstrumentTypeUpdate,
    LabCreate,
    LabOut,
    LabUpdate,
    LocationCreate,
    LocationOut,
    LocationUpdate,
    OrgUnitCreate,
    OrgUnitOut,
    OrgUnitUpdate,
    RegisterCheckEventIn,
    RegisterCheckEventOut,
    SpecialistCreate,
    SpecialistOut,
    SpecialistUpdate,
)

router = APIRouter()


async def _fetch_all(conn: AsyncConnection, stmt: str, params: dict) -> list[dict]:
    res = await conn.execute(text(stmt), params)
    return [dict(r._mapping) for r in res.fetchall()]


async def _fetch_one(conn: AsyncConnection, stmt: str, params: dict) -> dict | None:
    res = await conn.execute(text(stmt), params)
    row = res.fetchone()
    return dict(row._mapping) if row else None


def _build_update_sql(
    *,
    table: str,
    id_name: str,
    id_value: Any,
    data: dict[str, Any],
    allowed_cols: set[str],
    returning_cols: str,
) -> tuple[str, dict[str, Any]]:
    patch = {k: v for k, v in data.items() if k in allowed_cols}
    if not patch:
        raise HTTPException(status_code=400, detail="No fields to update")

    set_sql = ", ".join([f"{col} = :{col}" for col in patch.keys()])
    params = {**patch, id_name: id_value}
    sql = f"UPDATE {table} SET {set_sql} WHERE {id_name} = :{id_name} RETURNING {returning_cols}"
    return sql, params


@router.post("/org-units", response_model=OrgUnitOut)
async def create_org_unit(payload: OrgUnitCreate, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            """
            INSERT INTO metrology.org_unit(code, name, parent_id)
            VALUES (:code, :name, :parent_id)
            RETURNING id, code, name, parent_id
            """,
            payload.model_dump(),
        )
        assert row is not None
        return row


@router.get("/org-units", response_model=list[OrgUnitOut])
async def list_org_units(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    conn: AsyncConnection = Depends(get_conn),
):
    return await _fetch_all(
        conn,
        "SELECT id, code, name, parent_id FROM metrology.org_unit ORDER BY code LIMIT :limit OFFSET :offset",
        {"limit": limit, "offset": offset},
    )


@router.get("/org-units/{org_unit_id}", response_model=OrgUnitOut)
async def get_org_unit(org_unit_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    row = await _fetch_one(
        conn,
        "SELECT id, code, name, parent_id FROM metrology.org_unit WHERE id=:id",
        {"id": org_unit_id},
    )
    if not row:
        raise HTTPException(status_code=404, detail="org_unit not found")
    return row


@router.patch("/org-units/{org_unit_id}", response_model=OrgUnitOut)
async def update_org_unit(org_unit_id: UUID, payload: OrgUnitUpdate, conn: AsyncConnection = Depends(get_conn)):
    data = payload.model_dump(exclude_unset=True)
    sql, params = _build_update_sql(
        table="metrology.org_unit",
        id_name="id",
        id_value=org_unit_id,
        data=data,
        allowed_cols={"code", "name", "parent_id"},
        returning_cols="id, code, name, parent_id",
    )
    async with conn.begin():
        row = await _fetch_one(conn, sql, params)
        if not row:
            raise HTTPException(status_code=404, detail="org_unit not found")
        return row


@router.delete("/org-units/{org_unit_id}")
async def delete_org_unit(org_unit_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            "DELETE FROM metrology.org_unit WHERE id=:id RETURNING id",
            {"id": org_unit_id},
        )
        if not row:
            raise HTTPException(status_code=404, detail="org_unit not found")
        return {"status": "ok"}


@router.post("/locations", response_model=LocationOut)
async def create_location(payload: LocationCreate, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            """
            INSERT INTO metrology.location(org_unit_id, code, name)
            VALUES (:org_unit_id, :code, :name)
            RETURNING id, org_unit_id, code, name
            """,
            payload.model_dump(),
        )
        assert row is not None
        return row


@router.get("/locations", response_model=list[LocationOut])
async def list_locations(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    conn: AsyncConnection = Depends(get_conn),
):
    return await _fetch_all(
        conn,
        "SELECT id, org_unit_id, code, name FROM metrology.location ORDER BY code LIMIT :limit OFFSET :offset",
        {"limit": limit, "offset": offset},
    )


@router.get("/locations/{location_id}", response_model=LocationOut)
async def get_location(location_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    row = await _fetch_one(
        conn,
        "SELECT id, org_unit_id, code, name FROM metrology.location WHERE id=:id",
        {"id": location_id},
    )
    if not row:
        raise HTTPException(status_code=404, detail="location not found")
    return row


@router.patch("/locations/{location_id}", response_model=LocationOut)
async def update_location(location_id: UUID, payload: LocationUpdate, conn: AsyncConnection = Depends(get_conn)):
    data = payload.model_dump(exclude_unset=True)
    sql, params = _build_update_sql(
        table="metrology.location",
        id_name="id",
        id_value=location_id,
        data=data,
        allowed_cols={"org_unit_id", "code", "name"},
        returning_cols="id, org_unit_id, code, name",
    )
    async with conn.begin():
        row = await _fetch_one(conn, sql, params)
        if not row:
            raise HTTPException(status_code=404, detail="location not found")
        return row


@router.delete("/locations/{location_id}")
async def delete_location(location_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            "DELETE FROM metrology.location WHERE id=:id RETURNING id",
            {"id": location_id},
        )
        if not row:
            raise HTTPException(status_code=404, detail="location not found")
        return {"status": "ok"}


@router.post("/labs", response_model=LabOut)
async def create_lab(payload: LabCreate, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            """
            INSERT INTO metrology.lab(code, name, accreditation_no, contacts)
            VALUES (:code, :name, :accreditation_no, :contacts::jsonb)
            RETURNING id, code, name, accreditation_no, contacts
            """,
            payload.model_dump(),
        )
        assert row is not None
        return row


@router.get("/labs", response_model=list[LabOut])
async def list_labs(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    conn: AsyncConnection = Depends(get_conn),
):
    return await _fetch_all(
        conn,
        "SELECT id, code, name, accreditation_no, contacts FROM metrology.lab ORDER BY code LIMIT :limit OFFSET :offset",
        {"limit": limit, "offset": offset},
    )


@router.get("/labs/{lab_id}", response_model=LabOut)
async def get_lab(lab_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    row = await _fetch_one(
        conn,
        "SELECT id, code, name, accreditation_no, contacts FROM metrology.lab WHERE id=:id",
        {"id": lab_id},
    )
    if not row:
        raise HTTPException(status_code=404, detail="lab not found")
    return row


@router.patch("/labs/{lab_id}", response_model=LabOut)
async def update_lab(lab_id: UUID, payload: LabUpdate, conn: AsyncConnection = Depends(get_conn)):
    data = payload.model_dump(exclude_unset=True)
    allowed = {"code", "name", "accreditation_no", "contacts"}
    patch = {k: v for k, v in data.items() if k in allowed}
    if not patch:
        raise HTTPException(status_code=400, detail="No fields to update")

    sets = []
    for k in patch.keys():
        if k == "contacts":
            sets.append("contacts = :contacts::jsonb")
        else:
            sets.append(f"{k} = :{k}")
    set_sql = ", ".join(sets)

    async with conn.begin():
        row = await _fetch_one(
            conn,
            f"""
            UPDATE metrology.lab
            SET {set_sql}
            WHERE id = :id
            RETURNING id, code, name, accreditation_no, contacts
            """,
            {**patch, "id": lab_id},
        )
        if not row:
            raise HTTPException(status_code=404, detail="lab not found")
        return row


@router.delete("/labs/{lab_id}")
async def delete_lab(lab_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            "DELETE FROM metrology.lab WHERE id=:id RETURNING id",
            {"id": lab_id},
        )
        if not row:
            raise HTTPException(status_code=404, detail="lab not found")
        return {"status": "ok"}


@router.post("/specialists", response_model=SpecialistOut)
async def create_specialist(payload: SpecialistCreate, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            """
            INSERT INTO metrology.specialist(lab_id, full_name, position, email, phone)
            VALUES (:lab_id, :full_name, :position, :email, :phone)
            RETURNING id, lab_id, full_name, position, email, phone
            """,
            payload.model_dump(),
        )
        assert row is not None
        return row


@router.get("/specialists", response_model=list[SpecialistOut])
async def list_specialists(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    conn: AsyncConnection = Depends(get_conn),
):
    return await _fetch_all(
        conn,
        "SELECT id, lab_id, full_name, position, email, phone FROM metrology.specialist ORDER BY full_name LIMIT :limit OFFSET :offset",
        {"limit": limit, "offset": offset},
    )


@router.get("/specialists/{specialist_id}", response_model=SpecialistOut)
async def get_specialist(specialist_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    row = await _fetch_one(
        conn,
        "SELECT id, lab_id, full_name, position, email, phone FROM metrology.specialist WHERE id=:id",
        {"id": specialist_id},
    )
    if not row:
        raise HTTPException(status_code=404, detail="specialist not found")
    return row


@router.patch("/specialists/{specialist_id}", response_model=SpecialistOut)
async def update_specialist(
    specialist_id: UUID, payload: SpecialistUpdate, conn: AsyncConnection = Depends(get_conn)
):
    data = payload.model_dump(exclude_unset=True)
    sql, params = _build_update_sql(
        table="metrology.specialist",
        id_name="id",
        id_value=specialist_id,
        data=data,
        allowed_cols={"lab_id", "full_name", "position", "email", "phone"},
        returning_cols="id, lab_id, full_name, position, email, phone",
    )
    async with conn.begin():
        row = await _fetch_one(conn, sql, params)
        if not row:
            raise HTTPException(status_code=404, detail="specialist not found")
        return row


@router.delete("/specialists/{specialist_id}")
async def delete_specialist(specialist_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            "DELETE FROM metrology.specialist WHERE id=:id RETURNING id",
            {"id": specialist_id},
        )
        if not row:
            raise HTTPException(status_code=404, detail="specialist not found")
        return {"status": "ok"}


@router.post("/instrument-types", response_model=InstrumentTypeOut)
async def create_instrument_type(payload: InstrumentTypeCreate, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            """
            INSERT INTO metrology.instrument_type(code, name)
            VALUES (:code, :name)
            RETURNING id, code, name
            """,
            payload.model_dump(),
        )
        assert row is not None
        return row


@router.get("/instrument-types", response_model=list[InstrumentTypeOut])
async def list_instrument_types(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    conn: AsyncConnection = Depends(get_conn),
):
    return await _fetch_all(
        conn,
        "SELECT id, code, name FROM metrology.instrument_type ORDER BY code LIMIT :limit OFFSET :offset",
        {"limit": limit, "offset": offset},
    )


@router.get("/instrument-types/{instrument_type_id}", response_model=InstrumentTypeOut)
async def get_instrument_type(instrument_type_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    row = await _fetch_one(
        conn,
        "SELECT id, code, name FROM metrology.instrument_type WHERE id=:id",
        {"id": instrument_type_id},
    )
    if not row:
        raise HTTPException(status_code=404, detail="instrument_type not found")
    return row


@router.patch("/instrument-types/{instrument_type_id}", response_model=InstrumentTypeOut)
async def update_instrument_type(
    instrument_type_id: UUID, payload: InstrumentTypeUpdate, conn: AsyncConnection = Depends(get_conn)
):
    data = payload.model_dump(exclude_unset=True)
    sql, params = _build_update_sql(
        table="metrology.instrument_type",
        id_name="id",
        id_value=instrument_type_id,
        data=data,
        allowed_cols={"code", "name"},
        returning_cols="id, code, name",
    )
    async with conn.begin():
        row = await _fetch_one(conn, sql, params)
        if not row:
            raise HTTPException(status_code=404, detail="instrument_type not found")
        return row


@router.delete("/instrument-types/{instrument_type_id}")
async def delete_instrument_type(instrument_type_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            "DELETE FROM metrology.instrument_type WHERE id=:id RETURNING id",
            {"id": instrument_type_id},
        )
        if not row:
            raise HTTPException(status_code=404, detail="instrument_type not found")
        return {"status": "ok"}


@router.post("/instrument-models", response_model=InstrumentModelOut)
async def create_instrument_model(payload: InstrumentModelCreate, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            """
            INSERT INTO metrology.instrument_model(instrument_type_id, manufacturer, model_name, description)
            VALUES (:instrument_type_id, :manufacturer, :model_name, :description)
            RETURNING id, instrument_type_id, manufacturer, model_name, description
            """,
            payload.model_dump(),
        )
        assert row is not None
        return row


@router.get("/instrument-models", response_model=list[InstrumentModelOut])
async def list_instrument_models(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    conn: AsyncConnection = Depends(get_conn),
):
    return await _fetch_all(
        conn,
        """
        SELECT id, instrument_type_id, manufacturer, model_name, description
        FROM metrology.instrument_model
        ORDER BY manufacturer, model_name
        LIMIT :limit OFFSET :offset
        """,
        {"limit": limit, "offset": offset},
    )


@router.get("/instrument-models/{instrument_model_id}", response_model=InstrumentModelOut)
async def get_instrument_model(instrument_model_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    row = await _fetch_one(
        conn,
        """
        SELECT id, instrument_type_id, manufacturer, model_name, description
        FROM metrology.instrument_model
        WHERE id=:id
        """,
        {"id": instrument_model_id},
    )
    if not row:
        raise HTTPException(status_code=404, detail="instrument_model not found")
    return row


@router.patch("/instrument-models/{instrument_model_id}", response_model=InstrumentModelOut)
async def update_instrument_model(
    instrument_model_id: UUID, payload: InstrumentModelUpdate, conn: AsyncConnection = Depends(get_conn)
):
    data = payload.model_dump(exclude_unset=True)
    sql, params = _build_update_sql(
        table="metrology.instrument_model",
        id_name="id",
        id_value=instrument_model_id,
        data=data,
        allowed_cols={"instrument_type_id", "manufacturer", "model_name", "description"},
        returning_cols="id, instrument_type_id, manufacturer, model_name, description",
    )
    async with conn.begin():
        row = await _fetch_one(conn, sql, params)
        if not row:
            raise HTTPException(status_code=404, detail="instrument_model not found")
        return row


@router.delete("/instrument-models/{instrument_model_id}")
async def delete_instrument_model(instrument_model_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            "DELETE FROM metrology.instrument_model WHERE id=:id RETURNING id",
            {"id": instrument_model_id},
        )
        if not row:
            raise HTTPException(status_code=404, detail="instrument_model not found")
        return {"status": "ok"}


@router.post("/instruments", response_model=InstrumentOut)
async def create_instrument(payload: InstrumentCreate, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        status_row = await _fetch_one(
            conn,
            "SELECT id FROM metrology.instrument_status WHERE code=:code",
            {"code": payload.status_code},
        )
        if not status_row:
            raise HTTPException(status_code=400, detail="Unknown instrument status_code")

        data = payload.model_dump()
        data["status_id"] = status_row["id"]

        row = await _fetch_one(
            conn,
            """
            INSERT INTO metrology.instrument(
              instrument_model_id, inventory_no, serial_no,
              range_min, range_max, range_unit,
              error_limit, error_unit, accuracy_class,
              org_unit_id, location_id, installed_at, status_id
            )
            VALUES (
              :instrument_model_id, :inventory_no, :serial_no,
              :range_min, :range_max, :range_unit,
              :error_limit, :error_unit, :accuracy_class,
              :org_unit_id, :location_id, :installed_at, :status_id
            )
            RETURNING id, instrument_model_id, inventory_no, serial_no, org_unit_id, location_id, status_id, installed_at
            """,
            data,
        )
        assert row is not None
        return row


@router.get("/instruments", response_model=list[InstrumentOut])
async def list_instruments(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    conn: AsyncConnection = Depends(get_conn),
):
    return await _fetch_all(
        conn,
        """
        SELECT id, instrument_model_id, inventory_no, serial_no, org_unit_id, location_id, status_id, installed_at
        FROM metrology.instrument
        ORDER BY inventory_no
        LIMIT :limit OFFSET :offset
        """,
        {"limit": limit, "offset": offset},
    )


@router.get("/instruments/{instrument_id}", response_model=InstrumentOut)
async def get_instrument(instrument_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    row = await _fetch_one(
        conn,
        """
        SELECT id, instrument_model_id, inventory_no, serial_no, org_unit_id, location_id, status_id, installed_at
        FROM metrology.instrument
        WHERE id=:id
        """,
        {"id": instrument_id},
    )
    if not row:
        raise HTTPException(status_code=404, detail="instrument not found")
    return row


@router.patch("/instruments/{instrument_id}", response_model=InstrumentOut)
async def update_instrument(instrument_id: UUID, payload: InstrumentUpdate, conn: AsyncConnection = Depends(get_conn)):
    data = payload.model_dump(exclude_unset=True)
    async with conn.begin():
        if "status_code" in data:
            status_row = await _fetch_one(
                conn,
                "SELECT id FROM metrology.instrument_status WHERE code=:code",
                {"code": data["status_code"]},
            )
            if not status_row:
                raise HTTPException(status_code=400, detail="Unknown instrument status_code")
            data.pop("status_code", None)
            data["status_id"] = status_row["id"]

        sql, params = _build_update_sql(
            table="metrology.instrument",
            id_name="id",
            id_value=instrument_id,
            data=data,
            allowed_cols={
                "instrument_model_id",
                "inventory_no",
                "serial_no",
                "range_min",
                "range_max",
                "range_unit",
                "error_limit",
                "error_unit",
                "accuracy_class",
                "org_unit_id",
                "location_id",
                "installed_at",
                "status_id",
            },
            returning_cols="id, instrument_model_id, inventory_no, serial_no, org_unit_id, location_id, status_id, installed_at",
        )
        row = await _fetch_one(conn, sql, params)
        if not row:
            raise HTTPException(status_code=404, detail="instrument not found")
        return row


@router.delete("/instruments/{instrument_id}")
async def delete_instrument(instrument_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            "DELETE FROM metrology.instrument WHERE id=:id RETURNING id",
            {"id": instrument_id},
        )
        if not row:
            raise HTTPException(status_code=404, detail="instrument not found")
        return {"status": "ok"}


@router.post("/documents", response_model=DocumentOut)
async def create_document(payload: DocumentCreate, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        doc_type = await _fetch_one(
            conn,
            "SELECT id FROM metrology.document_type WHERE code=:code",
            {"code": payload.document_type_code},
        )
        if not doc_type:
            raise HTTPException(status_code=400, detail="Unknown document_type_code")

        row = await _fetch_one(
            conn,
            """
            INSERT INTO metrology.document(document_type_id, title, storage_ref, sha256)
            VALUES (:document_type_id, :title, :storage_ref, :sha256)
            RETURNING id, document_type_id, title, storage_ref, sha256, created_at
            """,
            {
                "document_type_id": doc_type["id"],
                "title": payload.title,
                "storage_ref": payload.storage_ref,
                "sha256": payload.sha256,
            },
        )
        assert row is not None
        return row


@router.get("/documents", response_model=list[DocumentOut])
async def list_documents(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    conn: AsyncConnection = Depends(get_conn),
):
    return await _fetch_all(
        conn,
        """
        SELECT id, document_type_id, title, storage_ref, sha256, created_at
        FROM metrology.document
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
        """,
        {"limit": limit, "offset": offset},
    )


@router.get("/documents/{document_id}", response_model=DocumentOut)
async def get_document(document_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    row = await _fetch_one(
        conn,
        """
        SELECT id, document_type_id, title, storage_ref, sha256, created_at
        FROM metrology.document
        WHERE id=:id
        """,
        {"id": document_id},
    )
    if not row:
        raise HTTPException(status_code=404, detail="document not found")
    return row


@router.patch("/documents/{document_id}", response_model=DocumentOut)
async def update_document(document_id: UUID, payload: DocumentUpdate, conn: AsyncConnection = Depends(get_conn)):
    data = payload.model_dump(exclude_unset=True)
    async with conn.begin():
        if "document_type_code" in data:
            doc_type = await _fetch_one(
                conn,
                "SELECT id FROM metrology.document_type WHERE code=:code",
                {"code": data["document_type_code"]},
            )
            if not doc_type:
                raise HTTPException(status_code=400, detail="Unknown document_type_code")
            data.pop("document_type_code", None)
            data["document_type_id"] = doc_type["id"]

        sql, params = _build_update_sql(
            table="metrology.document",
            id_name="id",
            id_value=document_id,
            data=data,
            allowed_cols={"document_type_id", "title", "storage_ref", "sha256"},
            returning_cols="id, document_type_id, title, storage_ref, sha256, created_at",
        )
        row = await _fetch_one(conn, sql, params)
        if not row:
            raise HTTPException(status_code=404, detail="document not found")
        return row


@router.delete("/documents/{document_id}")
async def delete_document(document_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            "DELETE FROM metrology.document WHERE id=:id RETURNING id",
            {"id": document_id},
        )
        if not row:
            raise HTTPException(status_code=404, detail="document not found")
        return {"status": "ok"}


@router.post("/check-events/register", response_model=RegisterCheckEventOut)
async def register_check_event(payload: RegisterCheckEventIn, conn: AsyncConnection = Depends(get_conn)):
    doc_ids = payload.document_ids or []
    async with conn.begin():
        row = await _fetch_one(
            conn,
            """
            SELECT metrology.fn_register_check_event(
              :instrument_id,
              :check_type_id,
              :check_date,
              :result_code,
              :lab_id,
              :specialist_id,
              :check_plan_id,
              :protocol_no,
              :notes,
              :document_ids
            ) AS event_id
            """,
            {**payload.model_dump(), "document_ids": doc_ids if doc_ids else None},
        )
        assert row is not None
        return row


@router.get("/check-events", response_model=list[CheckEventOut])
async def list_check_events(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    conn: AsyncConnection = Depends(get_conn),
):
    return await _fetch_all(
        conn,
        """
        SELECT id, instrument_id, check_plan_id, check_type_id, lab_id, specialist_id,
               check_date, result_status_id, protocol_no, next_due_date, notes, created_at
        FROM metrology.check_event
        ORDER BY check_date DESC, created_at DESC
        LIMIT :limit OFFSET :offset
        """,
        {"limit": limit, "offset": offset},
    )


@router.get("/check-events/{event_id}", response_model=CheckEventOut)
async def get_check_event(event_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    row = await _fetch_one(
        conn,
        """
        SELECT id, instrument_id, check_plan_id, check_type_id, lab_id, specialist_id,
               check_date, result_status_id, protocol_no, next_due_date, notes, created_at
        FROM metrology.check_event
        WHERE id=:id
        """,
        {"id": event_id},
    )
    if not row:
        raise HTTPException(status_code=404, detail="check_event not found")
    return row


@router.post("/check-types", response_model=CheckTypeOut)
async def create_check_type(payload: CheckTypeCreate, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        kind = await _fetch_one(
            conn,
            "SELECT id FROM metrology.check_kind WHERE code=:code",
            {"code": payload.kind_code},
        )
        if not kind:
            raise HTTPException(status_code=400, detail="Unknown kind_code")

        row = await _fetch_one(
            conn,
            """
            INSERT INTO metrology.check_type(code, name, check_kind_id)
            VALUES (:code, :name, :check_kind_id)
            RETURNING id, code, name, check_kind_id
            """,
            {"code": payload.code, "name": payload.name, "check_kind_id": kind["id"]},
        )
        assert row is not None
        return row


@router.get("/check-types", response_model=list[CheckTypeOut])
async def list_check_types(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    conn: AsyncConnection = Depends(get_conn),
):
    return await _fetch_all(
        conn,
        "SELECT id, code, name, check_kind_id FROM metrology.check_type ORDER BY code LIMIT :limit OFFSET :offset",
        {"limit": limit, "offset": offset},
    )


@router.get("/check-types/{check_type_id}", response_model=CheckTypeOut)
async def get_check_type(check_type_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    row = await _fetch_one(
        conn,
        "SELECT id, code, name, check_kind_id FROM metrology.check_type WHERE id=:id",
        {"id": check_type_id},
    )
    if not row:
        raise HTTPException(status_code=404, detail="check_type not found")
    return row


@router.patch("/check-types/{check_type_id}", response_model=CheckTypeOut)
async def update_check_type(check_type_id: UUID, payload: CheckTypeUpdate, conn: AsyncConnection = Depends(get_conn)):
    data = payload.model_dump(exclude_unset=True)
    async with conn.begin():
        if "kind_code" in data:
            kind = await _fetch_one(
                conn,
                "SELECT id FROM metrology.check_kind WHERE code=:code",
                {"code": data["kind_code"]},
            )
            if not kind:
                raise HTTPException(status_code=400, detail="Unknown kind_code")
            data.pop("kind_code", None)
            data["check_kind_id"] = kind["id"]

        sql, params = _build_update_sql(
            table="metrology.check_type",
            id_name="id",
            id_value=check_type_id,
            data=data,
            allowed_cols={"code", "name", "check_kind_id"},
            returning_cols="id, code, name, check_kind_id",
        )
        row = await _fetch_one(conn, sql, params)
        if not row:
            raise HTTPException(status_code=404, detail="check_type not found")
        return row


@router.delete("/check-types/{check_type_id}")
async def delete_check_type(check_type_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            "DELETE FROM metrology.check_type WHERE id=:id RETURNING id",
            {"id": check_type_id},
        )
        if not row:
            raise HTTPException(status_code=404, detail="check_type not found")
        return {"status": "ok"}


@router.post("/check-requirements", response_model=CheckRequirementOut)
async def create_check_requirement(payload: CheckRequirementCreate, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            """
            INSERT INTO metrology.check_requirement(
              instrument_model_id, check_type_id, interval_months, grace_days, is_mandatory, notes
            )
            VALUES (
              :instrument_model_id, :check_type_id, :interval_months, :grace_days, :is_mandatory, :notes
            )
            RETURNING id, instrument_model_id, check_type_id, interval_months, grace_days, is_mandatory, notes
            """,
            payload.model_dump(),
        )
        assert row is not None
        return row


@router.get("/check-requirements", response_model=list[CheckRequirementOut])
async def list_check_requirements(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    conn: AsyncConnection = Depends(get_conn),
):
    return await _fetch_all(
        conn,
        """
        SELECT id, instrument_model_id, check_type_id, interval_months, grace_days, is_mandatory, notes
        FROM metrology.check_requirement
        ORDER BY instrument_model_id, check_type_id
        LIMIT :limit OFFSET :offset
        """,
        {"limit": limit, "offset": offset},
    )


@router.get("/check-requirements/{requirement_id}", response_model=CheckRequirementOut)
async def get_check_requirement(requirement_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    row = await _fetch_one(
        conn,
        """
        SELECT id, instrument_model_id, check_type_id, interval_months, grace_days, is_mandatory, notes
        FROM metrology.check_requirement
        WHERE id=:id
        """,
        {"id": requirement_id},
    )
    if not row:
        raise HTTPException(status_code=404, detail="check_requirement not found")
    return row


@router.patch("/check-requirements/{requirement_id}", response_model=CheckRequirementOut)
async def update_check_requirement(
    requirement_id: UUID, payload: CheckRequirementUpdate, conn: AsyncConnection = Depends(get_conn)
):
    data = payload.model_dump(exclude_unset=True)
    sql, params = _build_update_sql(
        table="metrology.check_requirement",
        id_name="id",
        id_value=requirement_id,
        data=data,
        allowed_cols={"interval_months", "grace_days", "is_mandatory", "notes"},
        returning_cols="id, instrument_model_id, check_type_id, interval_months, grace_days, is_mandatory, notes",
    )
    async with conn.begin():
        row = await _fetch_one(conn, sql, params)
        if not row:
            raise HTTPException(status_code=404, detail="check_requirement not found")
        return row


@router.delete("/check-requirements/{requirement_id}")
async def delete_check_requirement(requirement_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            "DELETE FROM metrology.check_requirement WHERE id=:id RETURNING id",
            {"id": requirement_id},
        )
        if not row:
            raise HTTPException(status_code=404, detail="check_requirement not found")
        return {"status": "ok"}


@router.post("/check-plans", response_model=CheckPlanOut)
async def create_check_plan(payload: CheckPlanCreate, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        status = await _fetch_one(
            conn,
            "SELECT id FROM metrology.check_plan_status WHERE code='PLANNED'",
            {},
        )
        if not status:
            raise HTTPException(status_code=500, detail="check_plan_status not seeded")

        row = await _fetch_one(
            conn,
            """
            INSERT INTO metrology.check_plan(
              instrument_id, check_type_id, due_date,
              planned_lab_id, planned_specialist_id, status_id, notes
            )
            VALUES (
              :instrument_id, :check_type_id, :due_date,
              :planned_lab_id, :planned_specialist_id, :status_id, :notes
            )
            RETURNING id, instrument_id, check_type_id, due_date, planned_lab_id, planned_specialist_id, status_id, created_at, notes
            """,
            {**payload.model_dump(), "status_id": status["id"]},
        )
        assert row is not None
        return row


@router.get("/check-plans", response_model=list[CheckPlanOut])
async def list_check_plans(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    conn: AsyncConnection = Depends(get_conn),
):
    return await _fetch_all(
        conn,
        """
        SELECT id, instrument_id, check_type_id, due_date, planned_lab_id, planned_specialist_id,
               status_id, created_at, notes
        FROM metrology.check_plan
        ORDER BY due_date DESC
        LIMIT :limit OFFSET :offset
        """,
        {"limit": limit, "offset": offset},
    )


@router.get("/check-plans/{plan_id}", response_model=CheckPlanOut)
async def get_check_plan(plan_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    row = await _fetch_one(
        conn,
        """
        SELECT id, instrument_id, check_type_id, due_date, planned_lab_id, planned_specialist_id,
               status_id, created_at, notes
        FROM metrology.check_plan
        WHERE id=:id
        """,
        {"id": plan_id},
    )
    if not row:
        raise HTTPException(status_code=404, detail="check_plan not found")
    return row


@router.patch("/check-plans/{plan_id}", response_model=CheckPlanOut)
async def update_check_plan(plan_id: UUID, payload: CheckPlanUpdate, conn: AsyncConnection = Depends(get_conn)):
    data = payload.model_dump(exclude_unset=True)
    async with conn.begin():
        if "status_code" in data:
            status = await _fetch_one(
                conn,
                "SELECT id FROM metrology.check_plan_status WHERE code=:code",
                {"code": data["status_code"]},
            )
            if not status:
                raise HTTPException(status_code=400, detail="Unknown status_code")
            data.pop("status_code", None)
            data["status_id"] = status["id"]

        sql, params = _build_update_sql(
            table="metrology.check_plan",
            id_name="id",
            id_value=plan_id,
            data=data,
            allowed_cols={"due_date", "planned_lab_id", "planned_specialist_id", "notes", "status_id"},
            returning_cols="id, instrument_id, check_type_id, due_date, planned_lab_id, planned_specialist_id, status_id, created_at, notes",
        )
        row = await _fetch_one(conn, sql, params)
        if not row:
            raise HTTPException(status_code=404, detail="check_plan not found")
        return row


@router.delete("/check-plans/{plan_id}")
async def delete_check_plan(plan_id: UUID, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            "DELETE FROM metrology.check_plan WHERE id=:id RETURNING id",
            {"id": plan_id},
        )
        if not row:
            raise HTTPException(status_code=404, detail="check_plan not found")
        return {"status": "ok"}


@router.post("/instruments/{instrument_id}/decommission")
async def decommission_instrument(
    instrument_id: UUID,
    payload: DecommissionInstrumentIn,
    conn: AsyncConnection = Depends(get_conn),
):
    async with conn.begin():
        await conn.execute(
            text(
                """
                SELECT metrology.fn_decommission_instrument(
                  :instrument_id,
                  :reason,
                  :replaced_by_instrument_id
                )
                """
            ),
            {
                "instrument_id": instrument_id,
                "reason": payload.reason,
                "replaced_by_instrument_id": payload.replaced_by_instrument_id,
            },
        )
        return {"status": "ok"}


@router.post("/plans/generate", response_model=GeneratePlansOut)
async def generate_plans(payload: GeneratePlansIn, conn: AsyncConnection = Depends(get_conn)):
    async with conn.begin():
        row = await _fetch_one(
            conn,
            "SELECT metrology.fn_generate_check_plan(:from_date, :to_date) AS inserted",
            payload.model_dump(),
        )
        assert row is not None
        return row


@router.get("/reports/due-30d")
async def report_due_30d(conn: AsyncConnection = Depends(get_conn)):
    return await _fetch_all(
        conn,
        """
        SELECT *
        FROM metrology.mv_instruments_due_30d
        ORDER BY next_due_date, inventory_no
        """,
        {},
    )


@router.get("/reports/overdue")
async def report_overdue(conn: AsyncConnection = Depends(get_conn)):
    return await _fetch_all(
        conn,
        """
        SELECT *
        FROM metrology.mv_instruments_overdue
        ORDER BY next_due_date, inventory_no
        """,
        {},
    )


@router.get("/reports/by-lab")
async def report_by_lab(
    from_date: date | None = Query(default=None),
    to_date: date | None = Query(default=None),
    conn: AsyncConnection = Depends(get_conn),
):
    where = []
    params: dict = {}
    if from_date:
        where.append("ce.check_date >= :from_date")
        params["from_date"] = from_date
    if to_date:
        where.append("ce.check_date <= :to_date")
        params["to_date"] = to_date
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    return await _fetch_all(
        conn,
        f"""
        SELECT
          l.id AS lab_id,
          l.code AS lab_code,
          l.name AS lab_name,
          count(*) AS events_total,
          sum(CASE WHEN rs.is_success THEN 1 ELSE 0 END) AS events_passed,
          sum(CASE WHEN rs.is_success THEN 0 ELSE 1 END) AS events_not_passed
        FROM metrology.check_event ce
        JOIN metrology.lab l ON l.id = ce.lab_id
        JOIN metrology.check_result_status rs ON rs.id = ce.result_status_id
        {where_sql}
        GROUP BY l.id, l.code, l.name
        ORDER BY events_total DESC, l.code
        """,
        params,
    )


@router.get("/reports/by-org-unit")
async def report_by_org_unit(conn: AsyncConnection = Depends(get_conn)):
    return await _fetch_all(
        conn,
        """
        SELECT
          ou.id AS org_unit_id,
          ou.code AS org_unit_code,
          ou.name AS org_unit_name,
          count(i.id) AS instruments_total
        FROM metrology.org_unit ou
        LEFT JOIN metrology.instrument i ON i.org_unit_id = ou.id
        GROUP BY ou.id, ou.code, ou.name
        ORDER BY ou.code
        """,
        {},
    )


@router.get("/audit", response_model=list[AuditRowOut])
async def list_audit(
    table_name: str | None = Query(default=None),
    row_id: UUID | None = Query(default=None),
    since: datetime | None = Query(default=None),
    until: datetime | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
    conn: AsyncConnection = Depends(get_conn),
):
    where = []
    params: dict = {"limit": limit}
    if table_name:
        where.append("table_name = :table_name")
        params["table_name"] = table_name
    if row_id:
        where.append("row_id = :row_id")
        params["row_id"] = row_id
    if since:
        where.append('"at" >= :since')
        params["since"] = since
    if until:
        where.append('"at" < :until')
        params["until"] = until

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    return await _fetch_all(
        conn,
        f"""
        SELECT id, at, db_user, action, table_name, row_id, old_row, new_row
        FROM metrology.audit_log
        {where_sql}
        ORDER BY at DESC
        LIMIT :limit
        """,
        params,
    )


