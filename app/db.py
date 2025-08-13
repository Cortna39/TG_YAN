from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from contextlib import contextmanager
from app.settings import settings
import json

engine: Engine = create_engine(settings.database_url, pool_pre_ping=True, future=True)

@contextmanager
def conn():
    with engine.begin() as c:
        yield c

def get_deal_state(c, deal_id: int) -> dict | None:
    r = c.execute(text("SELECT * FROM deal_state WHERE deal_id=:id"), {"id": deal_id}).mappings().first()
    return dict(r) if r else None

def upsert_deal_state(c, **kwargs):
    q = text("""
        INSERT INTO deal_state
          (deal_id, last_stage_id, last_sent_hash, locked_counter_id, locked_mp_token, locked_uf_value, updated_at)
        VALUES
          (:deal_id, :last_stage_id, :last_sent_hash, :locked_counter_id, :locked_mp_token, :locked_uf_value, NOW())
        ON DUPLICATE KEY UPDATE
          last_stage_id = VALUES(last_stage_id),
          last_sent_hash = VALUES(last_sent_hash),
          locked_counter_id = IFNULL(locked_counter_id, VALUES(locked_counter_id)),
          locked_mp_token = IFNULL(locked_mp_token, VALUES(locked_mp_token)),
          locked_uf_value = IFNULL(locked_uf_value, VALUES(locked_uf_value)),
          updated_at = NOW()
    """)
    c.execute(q, kwargs)

def update_last_hash(c, deal_id: int, h: str):
    c.execute(text("UPDATE deal_state SET last_sent_hash=:h, updated_at=NOW() WHERE deal_id=:id"),
              {"h": h, "id": deal_id})

def enqueue(c, deal_id: int, event_type: str, payload: dict):
    c.execute(text("""
        INSERT INTO metrika_queue (deal_id, event_type, payload, status)
        VALUES (:deal_id, :event_type, :payload, 'queued')
    """), {"deal_id": deal_id, "event_type": event_type, "payload": json.dumps(payload, ensure_ascii=False)})

def fetch_queue_batch(c, limit: int = 50):
    return list(c.execute(text("""
        SELECT id, deal_id, event_type, payload, status, attempts, last_error, created_at, sent_at
        FROM metrika_queue
        WHERE status='queued'
        ORDER BY id
        LIMIT :limit
    """), {"limit": limit}).mappings())

def mark_sent(c, item_id: int):
    c.execute(text("UPDATE metrika_queue SET status='sent', sent_at=NOW() WHERE id=:id"), {"id": item_id})

def mark_error(c, item_id: int, msg: str):
    c.execute(text("""
        UPDATE metrika_queue
        SET status='error', attempts=attempts+1, last_error=:e
        WHERE id=:id
    """), {"e": msg[:1000], "id": item_id})

def get_routing_map(c) -> dict[str, dict]:
    rows = c.execute(text("SELECT uf_value, counter_id, mp_token FROM metrika_routing WHERE is_active=1")).mappings().all()
    m = {}
    for r in rows:
        m[str(r["uf_value"]).strip().lower()] = {"counter_id": int(r["counter_id"]), "mp_token": r["mp_token"]}
    return m
