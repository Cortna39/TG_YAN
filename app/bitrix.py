import requests
from app.settings import settings
from app.logger import get_logger

BX = settings.bitrix_webhook_url
log = get_logger("app.bitrix")


def bx_call(method: str, **params):
    try:
        r = requests.post(f"{BX}/{method}.json", json=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            raise RuntimeError(f"{method}: {data['error']}: {data.get('error_description')}")
        return data["result"]
    except Exception as e:
        log.error("bx_call_failed", extra={"method": method, "error": str(e)})
        raise


def get_deal_full(deal_id: int) -> dict:
    return bx_call("crm.deal.get", id=deal_id)


def get_deal_stage_id(deal: dict) -> str:
    return str(deal.get("STAGE_ID") or "")


def find_contact_by_comm(value: str, comm_type: str):
    if not value:
        return None
    try:
        res = bx_call("crm.duplicate.findbycomm", type=comm_type, values=[value])
    except Exception:
        res = bx_call("crm.duplicate.findbycomm", type=comm_type, values=[{"VALUE": value, "TYPE": comm_type}])
    ids = res.get("CONTACT", []) or []
    return int(ids[0]) if ids else None


def find_contact_by_uf_client_id(client_id: str | None, uf_code: str | None):
    if not (client_id and uf_code):
        return None
    res = bx_call("crm.contact.list", filter={uf_code: str(client_id)}, select=["ID"])
    if isinstance(res, list) and res and res[0].get("ID"):
        return int(res[0]["ID"])
    return None


def create_contact(name: str, phone: str | None, email: str | None,
                   client_id: str | None, uf_client_id_contact: str | None):
    fields = {"NAME": name or "Клиент", "OPENED": "Y"}
    if phone:
        fields["PHONE"] = [{"VALUE": phone, "VALUE_TYPE": "WORK"}]
    if email:
        fields["EMAIL"] = [{"VALUE": email, "VALUE_TYPE": "WORK"}]
    # если в Битриксе для client_id используется UF-поле контакта — сохраним туда
    if uf_client_id_contact and client_id:
        fields[uf_client_id_contact] = str(client_id)
    cid = int(bx_call("crm.contact.add", fields=fields))
    return cid


def link_contact_to_deal(deal_id: int, contact_id: int):
    bx_call("crm.deal.update", id=deal_id, fields={"CONTACT_ID": contact_id})


def extract_first_nonempty(entity: dict, keys: list[str]) -> str | None:
    for k in keys:
        v = entity.get(k)
        if isinstance(v, list):
            if v and isinstance(v[0], dict) and v[0].get("VALUE"):
                return str(v[0]["VALUE"])
        elif v:
            return str(v)
    return None


def ensure_contact_for_deal(deal: dict) -> int | None:
    """
    Находит/создаёт контакт и привязывает его к сделке.
    client_id теперь берём из обычного поля сделки 'client_id';
    если его нет — из UF-поля сделки, заданного в settings.uf_client_id_deal (обратная совместимость).
    """
    if deal.get("CONTACT_ID"):
        try:
            return int(deal["CONTACT_ID"])
        except Exception:
            return None

    phone = extract_first_nonempty(deal, ["PHONE", "UF_CRM_PHONE"])
    email = extract_first_nonempty(deal, ["EMAIL", "UF_CRM_EMAIL"])

    # ✅ ключевая правка: сперва обычное поле client_id, затем — старое UF-поле из настроек
    client_id = str(deal.get("client_id") or deal.get(settings.uf_client_id_deal) or "").strip()

    if settings.uf_client_id_contact and client_id:
        cid = find_contact_by_uf_client_id(client_id, settings.uf_client_id_contact)
        if cid:
            link_contact_to_deal(int(deal["ID"]), cid)
            log.info("contact_linked_by_uf", extra={"deal_id": deal.get("ID"), "contact_id": cid})
            return cid

    contact_id = None
    if phone:
        contact_id = find_contact_by_comm(phone, "PHONE")
    if not contact_id and email:
        contact_id = find_contact_by_comm(email, "EMAIL")

    if not contact_id:
        title = (deal.get("TITLE") or "Клиент").strip()
        contact_id = create_contact(title, phone, email, client_id or None, settings.uf_client_id_contact)

    link_contact_to_deal(int(deal["ID"]), contact_id)
    log.info("contact_linked", extra={"deal_id": deal.get("ID"), "contact_id": contact_id})
    return contact_id


def _first_comm(v):
    if isinstance(v, list) and v and isinstance(v[0], dict):
        return v[0].get("VALUE")
    return None


def get_contact_light(contact_id: int) -> dict:
    c = bx_call("crm.contact.get", id=contact_id)
    return {
        "ID": int(c.get("ID") or 0),
        "PHONE": _first_comm(c.get("PHONE")),
        "EMAIL": _first_comm(c.get("EMAIL")),
    }


def event_bind(event: str, handler: str):
    return bx_call("event.bind", event=event, handler=handler)


def event_unbind(event: str, handler: str):
    return bx_call("event.unbind", event=event, handler=handler)


_enum_cache = {}


def _load_enum_map(field_code: str) -> dict[int, str]:
    if field_code in _enum_cache:
        return _enum_cache[field_code]
    uf = bx_call("crm.deal.userfield.get", id=field_code)
    mapping: dict[int, str] = {}
    for item in uf.get("LIST", []) or []:
        try:
            enum_id = int(item.get("ID"))
        except Exception:
            continue
        xml_id = (item.get("XML_ID") or item.get("VALUE") or "").strip()
        mapping[enum_id] = xml_id
    _enum_cache[field_code] = mapping
    return mapping


from urllib.parse import urlparse

def _to_host(val: str) -> str:
    if not val:
        return ""
    s = val.strip()
    if not s.lower().startswith(("http://", "https://")):
        s = "https://" + s
    try:
        p = urlparse(s)
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""

def routing_value_from_deal(deal: dict, field_code: str) -> str:
    """
    Возвращает НОРМАЛИЗОВАННОЕ значение для маршрутизации:
    - если поле enum: берём XML_ID/значение и приводим к host
    - если строка/URL: приводим к host (без схемы/пути)
    """
    val = deal.get(field_code)
    if val is None or val == "":
        return ""
    # если это enum (число) — вытаскиваем XML_ID/значение как раньше
    try:
        enum_id = int(val)
        m = _load_enum_map(field_code)
        return _to_host(m.get(enum_id) or "")
    except Exception:
        # иначе строка
        return _to_host(str(val))
