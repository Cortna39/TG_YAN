from pydantic import BaseModel
import os, json
from datetime import date
from dateutil import parser as dtparser

def _parse_cutoff(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return dtparser.parse(s, dayfirst=True).date()
    except Exception:
        return None

class Settings(BaseModel):
    database_url: str = os.getenv("DATABASE_URL")

    bitrix_webhook_url: str = os.getenv("BITRIX_WEBHOOK_URL", "").rstrip("/")
    event_handler_url: str = os.getenv("EVENT_HANDLER_URL", "")

    uf_routing_field: str = os.getenv("UF_ROUTING_FIELD", "UF_CRM_BRAND")
    uf_client_id_deal: str = os.getenv("UF_CLIENT_ID_DEAL", "UF_CRM_CLIENT_ID")
    uf_required: str = os.getenv("UF_REQUIRED", "UF_CRM_SITE")
    uf_client_id_contact: str | None = os.getenv("UF_CLIENT_ID_CONTACT") or None

    paid_stages: list[str] = [s.strip() for s in os.getenv("PAID_STAGES", "").split(",") if s.strip()]
    cancelled_stages: list[str] = [s.strip() for s in os.getenv("CANCELLED_STAGES", "").split(",") if s.strip()]

    routing_default_behavior: str = os.getenv("ROUTING_DEFAULT_BEHAVIOR", "skip").lower()
    default_counter_id: int | None = int(os.getenv("DEFAULT_COUNTER_ID")) if os.getenv("DEFAULT_COUNTER_ID") else None
    default_mp_token: str | None = os.getenv("DEFAULT_MP_TOKEN") or None

    metrika_routing_json: list[dict] = json.loads(os.getenv("METRIKA_ROUTING_JSON", "[]"))

    process_from_date: date | None = _parse_cutoff(os.getenv("PROCESS_FROM_DATE"))

settings = Settings()
