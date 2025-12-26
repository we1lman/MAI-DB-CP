from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field


class OrgUnitCreate(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=256)
    parent_id: UUID | None = None


class OrgUnitUpdate(BaseModel):
    code: str | None = Field(default=None, min_length=1, max_length=64)
    name: str | None = Field(default=None, min_length=1, max_length=256)
    parent_id: UUID | None = None


class OrgUnitOut(BaseModel):
    id: UUID
    code: str
    name: str
    parent_id: UUID | None


class LocationCreate(BaseModel):
    org_unit_id: UUID
    code: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=256)


class LocationUpdate(BaseModel):
    org_unit_id: UUID | None = None
    code: str | None = Field(default=None, min_length=1, max_length=64)
    name: str | None = Field(default=None, min_length=1, max_length=256)


class LocationOut(BaseModel):
    id: UUID
    org_unit_id: UUID
    code: str
    name: str


class LabCreate(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=256)
    accreditation_no: str | None = None
    contacts: dict[str, Any] | None = None


class LabUpdate(BaseModel):
    code: str | None = Field(default=None, min_length=1, max_length=64)
    name: str | None = Field(default=None, min_length=1, max_length=256)
    accreditation_no: str | None = None
    contacts: dict[str, Any] | None = None


class LabOut(BaseModel):
    id: UUID
    code: str
    name: str
    accreditation_no: str | None
    contacts: dict[str, Any] | None


class SpecialistCreate(BaseModel):
    lab_id: UUID | None = None
    full_name: str = Field(min_length=1, max_length=256)
    position: str | None = None
    email: str | None = None
    phone: str | None = None


class SpecialistUpdate(BaseModel):
    lab_id: UUID | None = None
    full_name: str | None = Field(default=None, min_length=1, max_length=256)
    position: str | None = None
    email: str | None = None
    phone: str | None = None


class SpecialistOut(BaseModel):
    id: UUID
    lab_id: UUID | None
    full_name: str
    position: str | None
    email: str | None
    phone: str | None


class InstrumentTypeCreate(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=256)


class InstrumentTypeUpdate(BaseModel):
    code: str | None = Field(default=None, min_length=1, max_length=64)
    name: str | None = Field(default=None, min_length=1, max_length=256)


class InstrumentTypeOut(BaseModel):
    id: UUID
    code: str
    name: str


class InstrumentModelCreate(BaseModel):
    instrument_type_id: UUID
    manufacturer: str = Field(min_length=1, max_length=256)
    model_name: str = Field(min_length=1, max_length=256)
    description: str | None = None


class InstrumentModelUpdate(BaseModel):
    instrument_type_id: UUID | None = None
    manufacturer: str | None = Field(default=None, min_length=1, max_length=256)
    model_name: str | None = Field(default=None, min_length=1, max_length=256)
    description: str | None = None


class InstrumentModelOut(BaseModel):
    id: UUID
    instrument_type_id: UUID
    manufacturer: str
    model_name: str
    description: str | None


class InstrumentCreate(BaseModel):
    instrument_model_id: UUID
    inventory_no: str = Field(min_length=1, max_length=128)
    serial_no: str | None = None
    range_min: float | None = None
    range_max: float | None = None
    range_unit: str | None = None
    error_limit: float | None = None
    error_unit: str | None = None
    accuracy_class: str | None = None
    org_unit_id: UUID
    location_id: UUID
    installed_at: datetime | None = None
    status_code: Literal["ACTIVE", "IN_REPAIR", "DECOMMISSIONED", "REPLACED"] = "ACTIVE"


class InstrumentUpdate(BaseModel):
    instrument_model_id: UUID | None = None
    inventory_no: str | None = Field(default=None, min_length=1, max_length=128)
    serial_no: str | None = None
    range_min: float | None = None
    range_max: float | None = None
    range_unit: str | None = None
    error_limit: float | None = None
    error_unit: str | None = None
    accuracy_class: str | None = None
    org_unit_id: UUID | None = None
    location_id: UUID | None = None
    installed_at: datetime | None = None
    status_code: Literal["ACTIVE", "IN_REPAIR", "DECOMMISSIONED", "REPLACED"] | None = None


class InstrumentOut(BaseModel):
    id: UUID
    instrument_model_id: UUID
    inventory_no: str
    serial_no: str | None
    org_unit_id: UUID
    location_id: UUID
    status_id: UUID
    installed_at: datetime | None


class DocumentCreate(BaseModel):
    document_type_code: Literal["PROTOCOL", "CERTIFICATE", "OTHER"] = "PROTOCOL"
    title: str = Field(min_length=1, max_length=256)
    storage_ref: str = Field(min_length=1, max_length=2048)
    sha256: str | None = None


class DocumentUpdate(BaseModel):
    document_type_code: Literal["PROTOCOL", "CERTIFICATE", "OTHER"] | None = None
    title: str | None = Field(default=None, min_length=1, max_length=256)
    storage_ref: str | None = Field(default=None, min_length=1, max_length=2048)
    sha256: str | None = None


class DocumentOut(BaseModel):
    id: UUID
    document_type_id: UUID
    title: str
    storage_ref: str
    sha256: str | None
    created_at: datetime


class RegisterCheckEventIn(BaseModel):
    instrument_id: UUID
    check_type_id: UUID
    check_date: date
    result_code: Literal["PASSED", "FAILED", "CANCELED"]
    lab_id: UUID
    specialist_id: UUID | None = None
    check_plan_id: UUID | None = None
    protocol_no: str | None = None
    notes: str | None = None
    document_ids: list[UUID] | None = None


class RegisterCheckEventOut(BaseModel):
    event_id: UUID


class CheckEventOut(BaseModel):
    id: UUID
    instrument_id: UUID
    check_plan_id: UUID | None
    check_type_id: UUID
    lab_id: UUID
    specialist_id: UUID | None
    check_date: date
    result_status_id: UUID
    protocol_no: str | None
    next_due_date: date | None
    notes: str | None
    created_at: datetime


class CheckTypeCreate(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=256)
    kind_code: Literal["VERIFICATION", "CALIBRATION"]


class CheckTypeUpdate(BaseModel):
    code: str | None = Field(default=None, min_length=1, max_length=64)
    name: str | None = Field(default=None, min_length=1, max_length=256)
    kind_code: Literal["VERIFICATION", "CALIBRATION"] | None = None


class CheckTypeOut(BaseModel):
    id: UUID
    code: str
    name: str
    check_kind_id: UUID


class CheckRequirementCreate(BaseModel):
    instrument_model_id: UUID
    check_type_id: UUID
    interval_months: int = Field(ge=1, le=240)
    grace_days: int = Field(ge=0, le=3650)
    is_mandatory: bool = True
    notes: str | None = None


class CheckRequirementUpdate(BaseModel):
    interval_months: int | None = Field(default=None, ge=1, le=240)
    grace_days: int | None = Field(default=None, ge=0, le=3650)
    is_mandatory: bool | None = None
    notes: str | None = None


class CheckRequirementOut(BaseModel):
    id: UUID
    instrument_model_id: UUID
    check_type_id: UUID
    interval_months: int
    grace_days: int
    is_mandatory: bool
    notes: str | None


class CheckPlanCreate(BaseModel):
    instrument_id: UUID
    check_type_id: UUID
    due_date: date
    planned_lab_id: UUID | None = None
    planned_specialist_id: UUID | None = None
    notes: str | None = None


class CheckPlanUpdate(BaseModel):
    due_date: date | None = None
    planned_lab_id: UUID | None = None
    planned_specialist_id: UUID | None = None
    notes: str | None = None
    status_code: Literal["PLANNED", "DONE", "CANCELED"] | None = None


class CheckPlanOut(BaseModel):
    id: UUID
    instrument_id: UUID
    check_type_id: UUID
    due_date: date
    planned_lab_id: UUID | None
    planned_specialist_id: UUID | None
    status_id: UUID
    created_at: datetime
    notes: str | None


class DecommissionInstrumentIn(BaseModel):
    reason: str = Field(min_length=1, max_length=1024)
    replaced_by_instrument_id: UUID | None = None


class GeneratePlansIn(BaseModel):
    from_date: date
    to_date: date


class GeneratePlansOut(BaseModel):
    inserted: int


class AuditRowOut(BaseModel):
    id: UUID
    at: datetime
    db_user: str
    action: str
    table_name: str
    row_id: UUID | None
    old_row: dict[str, Any] | None
    new_row: dict[str, Any] | None


