from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Numeric, DateTime, func, text
from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional
import os

DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "change-me-in-production-123456")

class Base(DeclarativeBase):
    pass

class Transaction(Base):
    __tablename__ = "transactions_financieres"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: f"txn_{datetime.utcnow().timestamp()}")
    amount: Mapped[float] = mapped_column(Numeric(12, 2))
    currency: Mapped[str] = mapped_column(String(3), default="EUR")
    status: Mapped[str] = mapped_column(String(20), default="completed")
    external_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class SupportLog(Base):
    __tablename__ = "logs_support"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: f"log_{datetime.utcnow().timestamp()}")
    agent_id: Mapped[str] = mapped_column(String(100))
    agent_name: Mapped[Optional[str]] = mapped_column(String(150), nullable=True)
    status_connexion: Mapped[str] = mapped_column(String(20))
    messages_traites: Mapped[int] = mapped_column(default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

app = FastAPI(title="CRM Dashboard")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

class PaymentWebhook(BaseModel):
    amount: float
    currency: str = "EUR"
    status: str = "completed"
    external_id: Optional[str] = None

class SupportLogIn(BaseModel):
    agent_id: str
    agent_name: Optional[str] = None
    status_connexion: str
    messages_traites: int = 0

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
    async def broadcast(self, message: dict):
        for connection in self.active_connections[:]:
            try:
                await connection.send_json(message)
            except:
                self.disconnect(connection)

manager = ConnectionManager()

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@app.post("/webhook/payment")
async def payment_webhook(payload: PaymentWebhook, x_webhook_secret: str = Header(None)):
    if x_webhook_secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    async with async_session() as session:
        tx = Transaction(amount=payload.amount, currency=payload.currency, status=payload.status, external_id=payload.external_id)
        session.add(tx)
        await session.commit()
        await session.refresh(tx)
    await manager.broadcast({"type": "new_transaction", "data": {"id": tx.id, "amount": float(tx.amount), "currency": tx.currency, "status": tx.status, "created_at": tx.created_at.isoformat()}})
    return {"status": "success"}

@app.post("/api/support/log")
async def create_log(log: SupportLogIn):
    async with async_session() as session:
        entry = SupportLog(agent_id=log.agent_id, agent_name=log.agent_name, status_connexion=log.status_connexion, messages_traites=log.messages_traites)
        session.add(entry)
        await session.commit()
        await session.refresh(entry)
    await manager.broadcast({"type": "new_support_log", "data": {"id": entry.id, "agent_name": entry.agent_name, "status_connexion": entry.status_connexion, "messages_traites": entry.messages_traites, "created_at": entry.created_at.isoformat()}})
    return {"status": "success"}

@app.websocket("/ws/dashboard")
async def websocket_dashboard(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/api/metrics")
async def get_metrics():
    async with async_session() as session:
        row = (await session.execute(text("SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as count FROM transactions_financieres WHERE status='completed'"))).fetchone()
        txs = (await session.execute(text("SELECT * FROM transactions_financieres ORDER BY created_at DESC LIMIT 20"))).fetchall()
        logs = (await session.execute(text("SELECT * FROM logs_support ORDER BY created_at DESC LIMIT 30"))).fetchall()
    return {
        "total_revenue": float(row.total or 0),
        "total_transactions": row.count,
        "recent_transactions": [dict(r._mapping) for r in txs],
        "support_logs": [dict(r._mapping) for r in logs]
    }
