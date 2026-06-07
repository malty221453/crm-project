from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import os

app = FastAPI(title="CRM Backend")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "change-me-in-production-123456")

class PaymentWebhook(BaseModel):
    amount: float
    currency: str = "EUR"
    status: str = "completed"
    external_id: str = None

transactions = []

@app.post("/webhook/payment")
async def payment_webhook(payload: PaymentWebhook, x_webhook_secret: str = Header(None)):
    if x_webhook_secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")
    
    tx = {
        "amount": payload.amount,
        "currency": payload.currency,
        "status": payload.status,
        "external_id": payload.external_id
    }
    transactions.append(tx)
    return {"status": "success"}

@app.get("/api/metrics")
async def get_metrics():
    total = sum(t["amount"] for t in transactions if t["status"] == "completed")
    return {
        "total_revenue": total,
        "total_transactions": len(transactions),
        "recent_transactions": transactions[-10:]
    }

@app.websocket("/ws/dashboard")
async def websocket(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
