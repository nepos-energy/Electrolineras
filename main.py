import asyncio
import logging
from datetime import datetime, timezone
import os
import asyncpg
import websockets
from ocpp.routing import on
from ocpp.v16 import ChargePoint as cp
from ocpp.v16 import call_result
from ocpp.v16.enums import RegistrationStatus
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("neposcharge.ocpp")

DATABASE_URL = os.getenv("DATABASE_URL")
db_pool = None

async def get_db():
    return db_pool

class NeposChargePoint(cp):

    @on("BootNotification")
    async def on_boot_notification(self, charge_point_model, charge_point_vendor, **kwargs):
        logger.info(f"[{self.id}] BootNotification - Modelo: {charge_point_model}")
        try:
            db = await get_db()
            await db.execute("""
                INSERT INTO charge_points (ocpp_id, modelo, status, last_heartbeat)
                VALUES ($1, $2, 'Available', NOW())
                ON CONFLICT (ocpp_id) DO UPDATE
                SET modelo = $2, status = 'Available', last_heartbeat = NOW()
            """, self.id, charge_point_model)
            logger.info(f"[{self.id}] Registrado en base de datos")
        except Exception as e:
            logger.error(f"[{self.id}] Error DB: {e}")
        return call_result.BootNotification(
            current_time=datetime.now(timezone.utc).isoformat(),
            interval=30,
            status=RegistrationStatus.accepted
        )

    @on("Heartbeat")
    async def on_heartbeat(self, **kwargs):
        logger.info(f"[{self.id}] Heartbeat recibido")
        try:
            db = await get_db()
            await db.execute("""
                UPDATE charge_points SET last_heartbeat = NOW() WHERE ocpp_id = $1
            """, self.id)
        except Exception as e:
            logger.error(f"[{self.id}] Error DB heartbeat: {e}")
        return call_result.Heartbeat(
            current_time=datetime.now(timezone.utc).isoformat()
        )

    @on("StatusNotification")
    async def on_status_notification(self, connector_id, error_code, status, **kwargs):
        logger.info(f"[{self.id}] StatusNotification - Conector: {connector_id} | Estado: {status}")
        try:
            db = await get_db()
            await db.execute("""
                UPDATE charge_points SET status = $1 WHERE ocpp_id = $2
            """, status, self.id)
        except Exception as e:
            logger.error(f"[{self.id}] Error DB status: {e}")
        return call_result.StatusNotification()

async def on_connect(websocket):
    charge_point_id = websocket.request.path.strip("/")
    logger.info(f"Cargador conectado: {charge_point_id}")
    cp_instance = NeposChargePoint(charge_point_id, websocket)
    try:
        await cp_instance.start()
    except websockets.exceptions.ConnectionClosed:
        logger.info(f"Cargador desconectado: {charge_point_id}")

async def main():
    global db_pool
    logger.info("Conectando a base de datos...")
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    logger.info("Base de datos conectada")
    logger.info("Servidor OCPP iniciando en puerto 9000...")
    server = await websockets.serve(on_connect, "0.0.0.0", 9000, subprotocols=["ocpp1.6"])
    logger.info("Servidor OCPP listo - esperando cargadores")
    await server.wait_closed()

asyncio.run(main())
