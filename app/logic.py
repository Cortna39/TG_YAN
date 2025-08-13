from app.settings import settings
from app import bitrix as bx
from app.router import router
from app.db import conn, get_deal_state, upsert_deal_state, enqueue
from app.metrika import build_payload, payload_hash
from app.logger import get_logger
from app.utils import normalize_phone, sha256_hex
from dateutil import parser as dtparser

log = get_logger("app.logic")


def _is_before_cutoff(deal: dict) -> bool:
    if not settings.process_from_date:
        return False
    s = deal.get("DATE_CREATE") or ""
    try:
        created = dtparser.parse(s, dayfirst=False).date()
        return created < settings.process_from_date
    except Exception:
        return False


def _extract_client_id(deal: dict) -> str:
    """
    Берём client_id из обычного поля сделки 'client_id'.
    Если его нет — используем старое UF-поле из настроек (обратная совместимость).
    """
    return str(deal.get("client_id") or deal.get(settings.uf_client_id_deal) or "").strip()


def resolve_counter(deal: dict, state: dict | None):
    if state and state.get("locked_counter_id") and state.get("locked_mp_token"):
        return state["locked_counter_id"], state["locked_mp_token"], state.get("locked_uf_value") or ""
    uf_val = bx.routing_value_from_deal(deal, settings.uf_routing_field).lower()
    route = router.pick(uf_val)
    if route:
        return route["counter_id"], route["mp_token"], uf_val
    if settings.routing_default_behavior == "default" and settings.default_counter_id and settings.default_mp_token:
        return settings.default_counter_id, settings.default_mp_token, uf_val
    log.warning("no_routing", extra={"uf_value": uf_val, "deal_id": deal.get("ID")})
    raise RuntimeError(f"No routing for UF value: '{uf_val}'")


def has_required(deal: dict) -> bool:
    client_id = _extract_client_id(deal)
    req_val = bx.routing_value_from_deal(deal, settings.uf_required).strip()
    ok = bool(client_id and req_val)
    if not ok:
        log.warning("skip_no_required", extra={"deal_id": deal.get("ID")})
    return ok


def stage_to_event(stage_id: str) -> str | None:
    if stage_id in settings.paid_stages:
        return "deal_paid"
    if stage_id in settings.cancelled_stages:
        return "deal_cancelled"
    return None


def _contact_ep(contact_id: int | None) -> dict:
    if not contact_id:
        return {}
    c = bx.get_contact_light(contact_id)
    ep = {"contact_id": c.get("ID")}
    phone_norm = normalize_phone(c.get("PHONE") or "")
    email_norm = (c.get("EMAIL") or "").strip().lower()
    if phone_norm:
        ep["phash"] = sha256_hex(phone_norm)
    if email_norm:
        ep["ehash"] = sha256_hex(email_norm)
    return ep


def process_deal_event(event_type: str, deal_id: int):
    deal = bx.get_deal_full(deal_id)
    if _is_before_cutoff(deal):
        log.info("skip_old_deal", extra={"deal_id": deal_id, "date_create": deal.get("DATE_CREATE")})
        return

    contact_id = bx.ensure_contact_for_deal(deal)
    if not has_required(deal):
        return

    client_id = _extract_client_id(deal)

    with conn() as c:
        state = get_deal_state(c, deal_id)
        counter_id, token, used_uf = resolve_counter(deal, state)
        try:
            counter_id, token, used_uf = resolve_counter(deal, state)
        except RuntimeError as e:
            log.warning("resolve_counter_failed", extra={"deal_id": deal_id, "error": str(e)})
            return
        extra_ep = _contact_ep(contact_id)
        # передаём client_id, извлечённый из 'client_id' (или из UF при его отсутствии)
        payload = build_payload(counter_id, token, client_id, event_type, deal, used_uf, extra_ep=extra_ep)
        h = payload_hash(payload)
        if state and state.get("last_sent_hash") == h:
            log.info("dup_payload_skip", extra={"deal_id": deal_id, "event": event_type})
            return
        enqueue(c, deal_id, event_type, payload)
        upsert_deal_state(
            c,
            deal_id=deal_id,
            last_stage_id=deal.get("STAGE_ID"),
            last_sent_hash=h,
            locked_counter_id=state.get("locked_counter_id") if state else None,
            locked_mp_token=state.get("locked_mp_token") if state else None,
            locked_uf_value=state.get("locked_uf_value") if state else None,
        )
        log.info("queued_event", extra={"deal_id": deal_id, "event": event_type})


def handle_update(deal_id: int):
    deal = bx.get_deal_full(deal_id)
    if _is_before_cutoff(deal):
        log.info("skip_old_deal_update", extra={"deal_id": deal_id, "date_create": deal.get("DATE_CREATE")})
        return

    contact_id = bx.ensure_contact_for_deal(deal)
    if not has_required(deal):
        return

    client_id = _extract_client_id(deal)

    stg = bx.get_deal_stage_id(deal)
    ev = stage_to_event(stg)
    if ev:
        with conn() as c:
            state = get_deal_state(c, deal_id)
            counter_id, token, used_uf = resolve_counter(deal, state)
            try:
                counter_id, token, used_uf = resolve_counter(deal, state)
            except RuntimeError as e:
                log.warning("resolve_counter_failed", extra={"deal_id": deal_id, "error": str(e)})
                return
            extra_ep = _contact_ep(contact_id)
            # передаём актуальный client_id (из 'client_id' или из UF как fallback)
            payload = build_payload(counter_id, token, client_id, ev, deal, used_uf, extra_ep=extra_ep)
            h = payload_hash(payload)
            if state and state.get("last_sent_hash") == h:
                log.info("dup_payload_skip", extra={"deal_id": deal_id, "event": ev})
                return
            enqueue(c, deal_id, ev, payload)
            upsert_deal_state(
                c,
                deal_id=deal_id,
                last_stage_id=stg,
                last_sent_hash=h,
                locked_counter_id=(state.get("locked_counter_id") if state and state.get("locked_counter_id") else counter_id),
                locked_mp_token=(state.get("locked_mp_token") if state and state.get("locked_mp_token") else token),
                locked_uf_value=(state.get("locked_uf_value") if state and state.get("locked_uf_value") else used_uf),
            )
            log.info("queued_event", extra={"deal_id": deal_id, "event": ev})
