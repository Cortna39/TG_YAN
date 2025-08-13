from fastapi import FastAPI, Request
from threading import Thread
import time, os

from app.worker import worker_loop
from app.logic import process_deal_event, handle_update
from app.settings import settings
from app.logger import configure_root, get_logger

configure_root("app.log")
log = get_logger("app.main")

app = FastAPI(title="Bitrix→Metrika MP")

t = Thread(target=worker_loop, daemon=True, name="metrika-worker")
t.start()

MAX_BODY = int(os.getenv("LOG_REQUEST_BODY_MAX", "2048"))

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    try:
        body = await request.body()
        body_preview = body[:MAX_BODY].decode("utf-8", "ignore") if body else ""
    except Exception:
        body_preview = "<unreadable>"
    resp = None
    try:
        resp = await call_next(request)
        return resp
    finally:
        duration_ms = int((time.time() - start) * 1000)
        log.info(
            "http_request",
            extra={
                "path": request.url.path,
                "query": str(request.url.query),
                "method": request.method,
                "status": getattr(resp, "status_code", 0),
                "duration_ms": duration_ms,
                "body": body_preview
            }
        )

@app.post("/bitrix/events")
async def bitrix_events(request: Request):
    out_token = os.getenv("BITRIX_OUTHOOK_TOKEN")
    if out_token:
        hdr = request.headers.get("X-Hook-Token") or request.headers.get("X-Webhook-Token")
        if hdr != out_token:
            log.warning("forbidden_webhook", extra={"hdr": hdr})
            return {"ok": False, "error": "forbidden"}

    data = await request.json()
    event = data.get("event")
    fields = (data.get("data") or {}).get("FIELDS") or {}
    deal_id = int(fields.get("ID") or fields.get("dealId") or 0)

    if not (event and deal_id):
        log.warning("bad_event_payload", extra={"payload": data})
        return {"ok": True}

    log.info("event_received", extra={"event": event, "deal_id": deal_id})

    if event == "onCrmDealAdd":
        log.info("before_handle_create", extra={"deal_id": deal_id})
        process_deal_event("deal_created", deal_id)
    elif event == "onCrmDealUpdate":
        log.info("before_handle_update", extra={"deal_id": deal_id})
        handle_update(deal_id)

    return {"ok": True, "event": event, "deal_id": deal_id}

@app.get("/health")
def health():
    return {"ok": True, "paid": settings.paid_stages, "cancel": settings.cancelled_stages}
