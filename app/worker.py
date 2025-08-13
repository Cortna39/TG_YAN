import time, json
from app.db import conn, fetch_queue_batch, mark_sent, mark_error, get_deal_state, update_last_hash
from app.metrika import send, payload_hash
from app.logger import get_logger, configure_root

configure_root("worker.log")
log = get_logger("app.worker")

def _as_dict(val):
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val)
    except Exception:
        return val

def worker_loop():
    log.info("worker_started")
    while True:
        try:
            with conn() as c:
                batch = fetch_queue_batch(c, limit=50)
            if not batch:
                time.sleep(2)
                continue
            for item in batch:
                try:
                    payload = _as_dict(item["payload"])
                    send(payload)
                    with conn() as c2:
                        mark_sent(c2, item["id"])
                        h = payload_hash(payload if isinstance(payload, dict) else {})
                        st = get_deal_state(c2, item["deal_id"])
                        if st:
                            update_last_hash(c2, item["deal_id"], h)
                    log.info("event_sent", extra={"queue_id": item["id"], "deal_id": item["deal_id"], "type": item["event_type"]})
                except Exception as e:
                    with conn() as c3:
                        mark_error(c3, item["id"], str(e))
                    log.error("event_send_failed", extra={"queue_id": item["id"], "error": str(e)})
                    time.sleep(0.5)
        except Exception:
            log.exception("worker_loop_error")
            time.sleep(2)
