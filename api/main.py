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
            kwh_requested, payment_method, payment_status, session_status,
            started_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, NOW())
        RETURNING id, session_status, started_at
        """,
        cp_row["id"], body.connector_id, tenant_id, body.user_token,
        body.kwh_requested, body.payment_method, SessionStatus.PENDING_PAYMENT.value,
    )
    logger.info(f"Sesion creada: {row['id']} (tenant={tenant_id})")
    return {
        "id": str(row["id"]),
        "session_status": row["session_status"],
        "started_at": row["started_at"],
    }


@app.get("/sessions/{session_id}", response_model=SessionResponse)
async def get_session(
    session_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: asyncpg.Pool = Depends(get_db),
):
    """
    Devuelve el estado de la sesión + el último meter_value asociado.
    Pensado para polling desde el Kiosk durante la carga: una sola
    llamada trae session_status, kwh_delivered y power_kw actuales.
    """
    session = await db.fetchrow(
        """
        SELECT s.*, cp.ocpp_id as charge_point_ocpp_id
        FROM sessions s
        JOIN charge_points cp ON cp.id = s.charge_point_id
        WHERE s.id = $1 AND s.tenant_id = $2
        """,
        session_id, tenant_id,
    )
    if not session:
        raise HTTPException(status_code=404, detail="Sesion no encontrada")

    last_meter = await db.fetchrow(
        """
        SELECT kwh, power_kw, voltage, current_a, soc_pct, timestamp
        FROM meter_values
        WHERE session_id = $1
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        session_id,
    )

    return {
        "id": str(session["id"]),
        "tenant_id": session["tenant_id"],
        "charge_point_id": session["charge_point_ocpp_id"],
        "connector_id": session["connector_id"],
        "session_status": session["session_status"],
        "payment_status": session["payment_status"],
        "payment_method": session["payment_method"],
        "kwh_requested": session["kwh_requested"],
        "kwh_delivered": session["kwh_delivered"],
        "amount_charged": session["amount_charged"],
        "started_at": session["started_at"],
        "ended_at": session["ended_at"],
        "last_meter": dict(last_meter) if last_meter else None,
    }


@app.patch("/sessions/{session_id}")
async def update_session(
    session_id: str,
    body: UpdateSessionRequest,
    tenant_id: str = Depends(get_tenant_id),
    db: asyncpg.Pool = Depends(get_db),
):
    """
    Transiciona el estado de una sesión. Valida que la transición sea
    permitida según la máquina de estados — si no, devuelve 409 Conflict.
    """
    current = await db.fetchrow(
        "SELECT session_status FROM sessions WHERE id = $1 AND tenant_id = $2",
        session_id, tenant_id,
    )
    if not current:
        raise HTTPException(status_code=404, detail="Sesion no encontrada")

    validate_transition(current["session_status"], body.session_status.value)

    # Si pasa a estado terminal, registrar ended_at
    extra_set = ""
    if body.session_status in (SessionStatus.COMPLETED, SessionStatus.FAILED, SessionStatus.TIMEOUT):
        extra_set = ", ended_at = NOW()"

    row = await db.fetchrow(
        f"""
        UPDATE sessions
        SET session_status = $1 {extra_set}
        WHERE id = $2 AND tenant_id = $3
        RETURNING id, session_status, ended_at
        """,
        body.session_status.value, session_id, tenant_id,
    )
    logger.info(
        f"Sesion {session_id}: {current['session_status']} -> {body.session_status.value}"
    )
    return {
        "id": str(row["id"]),
        "session_status": row["session_status"],
        "ended_at": row["ended_at"],
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Endpoints — Chargers / Connectors
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/chargers")
async def list_chargers(
    tenant_id: str = Depends(get_tenant_id),
    db: asyncpg.Pool = Depends(get_db),
):
    rows = await db.fetch(
        """
        SELECT id, ocpp_id, modelo, vendor, status, last_heartbeat, activo
        FROM charge_points
        WHERE tenant_id = $1
        ORDER BY ocpp_id
        """,
        tenant_id,
    )
    return [dict(r) for r in rows]


@app.get("/connectors")
async def list_connectors(
    charge_point_id: Optional[str] = None,
    tenant_id: str = Depends(get_tenant_id),
    db: asyncpg.Pool = Depends(get_db),
):
    if charge_point_id:
        rows = await db.fetch(
            """
            SELECT c.id, c.connector_id, c.tipo, c.status, c.updated_at
            FROM connectors c
            JOIN charge_points cp ON cp.id = c.charge_point_id
            WHERE cp.ocpp_id = $1 AND cp.tenant_id = $2
            ORDER BY c.connector_id
            """,
            charge_point_id, tenant_id,
        )
    else:
        rows = await db.fetch(
            """
            SELECT c.id, c.connector_id, c.tipo, c.status, c.updated_at,
                   cp.ocpp_id as charge_point_id
            FROM connectors c
            JOIN charge_points cp ON cp.id = c.charge_point_id
            WHERE cp.tenant_id = $1
            ORDER BY cp.ocpp_id, c.connector_id
            """,
            tenant_id,
        )
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
#  Webhook — Pagoplux (placeholder con validación de firma desde T16)
# ─────────────────────────────────────────────────────────────────────────────
def verify_pagoplux_signature(raw_body: bytes, signature_header: Optional[str]) -> bool:
    """
    Valida la firma del webhook de Pagoplux usando HMAC-SHA256.

    NOTA: El algoritmo exacto y el nombre del header dependen de la
    documentación de Pagoplux — esto es la ESTRUCTURA de validación,
    que se ajusta en T19 con las specs reales del proveedor. Lo
    importante es que el endpoint NUNCA procese un webhook sin firma
    válida, ni siquiera en desarrollo.
    """
    if not signature_header:
        return False

    expected = hmac.new(
        PAGOPLUX_WEBHOOK_SECRET.encode(),
        raw_body,
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(expected, signature_header)


@app.post("/webhooks/pagoplux")
async def pagoplux_webhook(
    request: Request,
    x_pagoplux_signature: Optional[str] = Header(default=None),
    db: asyncpg.Pool = Depends(get_db),
):
    """
    Recibe la confirmación de pago de Pagoplux.

    FLUJO (completo en T19):
    1. Validar firma (acá, ya implementado)
    2. Buscar el payment por external_id
    3. Marcar payment.status = 'confirmed', webhook_received_at = NOW()
    4. Transicionar session: PENDING_PAYMENT -> PAYMENT_CONFIRMED
    5. Disparar RemoteStartTransaction al servidor OCPP (T17)

    HOY (T16): solo validación de firma + logging. La lógica de negocio
    completa se implementa en T19 cuando tengamos las credenciales y
    el formato real de payload de Pagoplux.
    """
    raw_body = await request.body()

    if not verify_pagoplux_signature(raw_body, x_pagoplux_signature):
        logger.warning("Webhook Pagoplux recibido con firma invalida — rechazado")
        raise HTTPException(status_code=401, detail="Firma invalida")

    payload = await request.json()
    logger.info(f"Webhook Pagoplux recibido (firma OK): {payload}")

    # TODO T19: procesar payload real de Pagoplux
    #   - extraer external_id, status, amount
    #   - UPDATE payments SET status=..., webhook_received_at=NOW(), raw_webhook=...
    #   - validate_transition + UPDATE sessions SET session_status='PAYMENT_CONFIRMED'
    #   - llamar al servidor OCPP para RemoteStartTransaction

    return {"received": True}


# ─────────────────────────────────────────────────────────────────────────────
#  Health check
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "service": "neposcharge-api", "time": datetime.now(timezone.utc).isoformat()}
