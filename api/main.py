"""
NeposCharge API REST — T16
============================
API REST con FastAPI para NeposCharge.

Endpoints principales:
- POST   /sessions              -> crear sesión de carga (PENDING_PAYMENT)
- GET    /sessions/{id}          -> estado de sesión + último meter_value
- PATCH  /sessions/{id}          -> transición de estado (validada)
- GET    /chargers                -> lista de cargadores (con estado)
- GET    /connectors              -> lista de conectores
- POST   /webhooks/pagoplux       -> webhook de confirmación de pago (firma validada)

Diseño aplicado según recomendaciones del CTO:
1. Máquina de estados validada — no se aceptan transiciones arbitrarias.
2. tenant_id se obtiene del contexto de autenticación (header simulado
   por ahora), nunca del body que manda el cliente.
3. GET /sessions/{id} devuelve también el último meter_value para
   evitar una segunda llamada desde el Kiosk durante el polling.
4. El webhook de Pagoplux valida firma desde el día 1 (aunque la
   lógica real de Pagoplux se implementa en T19).
"""

import os
import hmac
import hashlib
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import asyncpg
from fastapi import FastAPI, HTTPException, Depends, Header, Request
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("neposcharge.api")

DATABASE_URL = os.getenv("DATABASE_URL")
PAGOPLUX_WEBHOOK_SECRET = os.getenv("PAGOPLUX_WEBHOOK_SECRET", "CHANGE-ME-IN-PRODUCTION")

app = FastAPI(title="NeposCharge API", version="0.1.0")

db_pool: Optional[asyncpg.Pool] = None


# ─────────────────────────────────────────────────────────────────────────────
#  Lifecycle
# ─────────────────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    global db_pool
    logger.info("Conectando a base de datos...")
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    logger.info("Base de datos conectada")


@app.on_event("shutdown")
async def shutdown():
    if db_pool:
        await db_pool.close()


async def get_db() -> asyncpg.Pool:
    if db_pool is None:
        raise HTTPException(status_code=503, detail="Base de datos no disponible")
    return db_pool


# ─────────────────────────────────────────────────────────────────────────────
#  Auth — placeholder de tenant_id desde JWT
# ─────────────────────────────────────────────────────────────────────────────
async def get_tenant_id(x_tenant_id: Optional[str] = Header(default=None)) -> str:
    """
    Punto único de obtención del tenant_id.

    HOY: se simula leyendo un header 'X-Tenant-Id' (solo para desarrollo).
    FUTURO (Etapa 2/3): se reemplaza por decodificación del JWT de Supabase
    Auth -> tenant_id = payload["tenant_id"]. El cliente NUNCA debe poder
    mandar tenant_id como parámetro de body — eso permitiría a un
    franquiciado crear sesiones en el tenant de otro.

    Mantener esta función como único punto de entrada hace que el cambio
    a JWT real sea un cambio de una sola función, no de todos los endpoints.
    """
    if not x_tenant_id:
        raise HTTPException(
            status_code=401,
            detail="Falta tenant_id (header X-Tenant-Id) — en producción vendrá del JWT"
        )
    return x_tenant_id


# ─────────────────────────────────────────────────────────────────────────────
#  Máquina de estados de sesión
# ─────────────────────────────────────────────────────────────────────────────
class SessionStatus(str, Enum):
    PENDING_PAYMENT = "PENDING_PAYMENT"
    PAYMENT_CONFIRMED = "PAYMENT_CONFIRMED"
    STARTING = "STARTING"
    CHARGING = "CHARGING"
    STOPPING = "STOPPING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"


# Transiciones permitidas: estado_actual -> {estados_destino_validos}
VALID_TRANSITIONS: dict[SessionStatus, set[SessionStatus]] = {
    SessionStatus.PENDING_PAYMENT: {
        SessionStatus.PAYMENT_CONFIRMED,
        SessionStatus.FAILED,
        SessionStatus.TIMEOUT,
    },
    SessionStatus.PAYMENT_CONFIRMED: {
        SessionStatus.STARTING,
        SessionStatus.FAILED,
        SessionStatus.TIMEOUT,
    },
    SessionStatus.STARTING: {
        SessionStatus.CHARGING,
        SessionStatus.FAILED,
        SessionStatus.TIMEOUT,
    },
    SessionStatus.CHARGING: {
        SessionStatus.STOPPING,
        SessionStatus.FAILED,
    },
    SessionStatus.STOPPING: {
        SessionStatus.COMPLETED,
        SessionStatus.FAILED,
    },
    # Estados terminales — sin transiciones salientes
    SessionStatus.COMPLETED: set(),
    SessionStatus.FAILED: set(),
    SessionStatus.TIMEOUT: set(),
}


def validate_transition(current: str, target: str) -> None:
    """
    Lanza HTTPException 409 si la transición current -> target no es válida.
    """
    try:
        current_enum = SessionStatus(current)
        target_enum = SessionStatus(target)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Estado inválido: {current} o {target}")

    allowed = VALID_TRANSITIONS.get(current_enum, set())
    if target_enum not in allowed:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Transición no permitida: {current} -> {target}. "
                f"Transiciones válidas desde {current}: "
                f"{[s.value for s in allowed] if allowed else 'ninguna (estado terminal)'}"
            ),
        )


# ─────────────────────────────────────────────────────────────────────────────
#  Schemas
# ─────────────────────────────────────────────────────────────────────────────
class CreateSessionRequest(BaseModel):
    # NOTA: tenant_id NO va acá — se obtiene de get_tenant_id() (auth)
    charge_point_id: str = Field(..., description="ID OCPP del cargador, ej: EVINKA-001")
    connector_id: int = Field(..., ge=1, description="Número de conector")
    user_token: Optional[str] = Field(default=None, description="Token de usuario/RFID, opcional")
    kwh_requested: Optional[float] = Field(default=None, ge=0)
    payment_method: str = Field(..., description="card | qr | wallet")


class UpdateSessionRequest(BaseModel):
    session_status: SessionStatus


class SessionResponse(BaseModel):
    id: str
    tenant_id: str
    charge_point_id: str
    connector_id: int
    session_status: str
    payment_status: Optional[str]
    payment_method: Optional[str]
    kwh_requested: Optional[float]
    kwh_delivered: Optional[float]
    amount_charged: Optional[float]
    started_at: Optional[datetime]
    ended_at: Optional[datetime]
    # Último meter_value embebido — evita una segunda llamada del Kiosk
    last_meter: Optional[dict]


# ─────────────────────────────────────────────────────────────────────────────
#  Endpoints — Sessions
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/sessions", status_code=201)
async def create_session(
    body: CreateSessionRequest,
    tenant_id: str = Depends(get_tenant_id),
    db: asyncpg.Pool = Depends(get_db),
):
    """
    Crea una nueva sesión de carga en estado PENDING_PAYMENT.
    tenant_id viene de la autenticación, NUNCA del body.
    """
    # Verificar que el charge_point pertenece al tenant
    cp_row = await db.fetchrow(
        "SELECT id FROM charge_points WHERE ocpp_id = $1 AND tenant_id = $2",
        body.charge_point_id, tenant_id,
    )
    if not cp_row:
        raise HTTPException(status_code=404, detail="Cargador no encontrado para este tenant")

    row = await db.fetchrow(
        """
        INSERT INTO sessions (
            charge_point_id, connector_id, tenant_id, user_token,
            kwh_requested, payment_method, payment_status, se
