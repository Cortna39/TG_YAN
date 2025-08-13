import os
import time
import requests, hashlib, json
from requests import exceptions
from datetime import datetime, timezone
from app.logger import get_logger

MC_URL = "https://mc.yandex.ru/collect"
log = get_logger("app.metrika")

METRIKA_MAX_ATTEMPTS = int(os.getenv("METRIKA_MAX_ATTEMPTS", "3"))

def build_payload(counter_id: int, token: str, client_id: str, event_name: str, deal: dict, uf_value: str, extra_ep: dict | None = None):
    ts = int(datetime.now(timezone.utc).timestamp())
    payload = {
        "tid": counter_id,
        "cid": str(client_id),
        "t": "event",
        "ea": event_name,
        "ti": f"DEAL_{deal['ID']}",
        "et": ts,
        "ms": token,
        "ep.uf_value": uf_value
    }
    if event_name == "deal_paid":
        if deal.get("OPPORTUNITY"):
            payload["tr"] = str(deal["OPPORTUNITY"])
        if deal.get("CURRENCY_ID"):
            payload["cu"] = deal["CURRENCY_ID"]
    if extra_ep:
        for k, v in extra_ep.items():
            if v is None or v == "":
                continue
            payload[f"ep.{k}"] = str(v)
    return payload

def send(payload: dict):
    for attempt in range(1, METRIKA_MAX_ATTEMPTS + 1):
        try:
            r = requests.post(MC_URL, data=payload, timeout=10)
        except exceptions.RequestException as e:
            if attempt == METRIKA_MAX_ATTEMPTS:
                log.error("mp_failed",
                          extra={"counter": payload.get("tid"), "event": payload.get("ea"), "deal": payload.get("ti"),
                                 "error": str(e), "attempt": attempt})
                raise
            log.warning("mp_retry",
                        extra={"counter": payload.get("tid"), "event": payload.get("ea"), "deal": payload.get("ti"),
                               "error": str(e), "attempt": attempt})
            time.sleep(2 ** (attempt - 1))
            continue
        try:
            r.raise_for_status()
            log.info("mp_sent",
                     extra={"counter": payload.get("tid"), "event": payload.get("ea"), "deal": payload.get("ti")})
            return True
        except Exception as e:
            log.error("mp_failed",
                      extra={"counter": payload.get("tid"), "event": payload.get("ea"), "deal": payload.get("ti"),
                             "error": str(e)})
            raise
def payload_hash(payload: dict) -> str:
    s = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()
