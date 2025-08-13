from fastapi import FastAPI, Request
from threading import Thread
import json
import asyncio
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

LOG_BODY = os.getenv("LOG_REQUEST_BODY", "true").lower() != "false"
MAX_BODY = int(os.getenv("LOG_REQUEST_BODY_MAX", "2048"))
SENSITIVE_FIELDS = {"password", "token", "secret", "authorization"}


def _mask_sensitive(body: str) -> str:
    try:
        data = json.loads(body)
    except Exception:
        return body

    def _mask(obj):
        if isinstance(obj, dict):
            return {
                k: ("<redacted>" if k.lower() in SENSITIVE_FIELDS else _mask(v))
                for k, v in obj.items()
            }
        if isinstance(obj, list):
            return [_mask(v) for v in obj]
        return obj

    return json.dumps(_mask(data))

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    body_preview = None
    if LOG_BODY:
        try:
            body_bytes = await request.body()
            if body_bytes:
                body_preview = _mask_sensitive(body_bytes.decode("utf-8", "ignore"))[:MAX_BODY]
            else:
                body_preview = ""
        except Exception:
            body_preview = "<unreadable>"
    resp = None
    try:
        resp = await call_next(request)
        return resp
    finally:
        duration_ms = int((time.time() - start) * 1000)
        extra = {
            "path": request.url.path,
            "query": str(request.url.query),
            "method": request.method,
            "status": getattr(resp, "status_code", 0),
            "duration_ms": duration_ms,
        }
        if LOG_BODY and body_preview is not None:
            extra["body"] = body_preview
        log.info("http_request", extra=extra)

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
        await asyncio.to_thread(process_deal_event, "deal_created", deal_id)
    elif event == "onCrmDealUpdate":
        log.info("before_handle_update", extra={"deal_id": deal_id})
        await asyncio.to_thread(handle_update, deal_id)

    return {"ok": True, "event": event, "deal_id": deal_id}

@app.get("/health")
def health():
    return {"ok": True, "paid": settings.paid_stages, "cancel": settings.cancelled_stages}
