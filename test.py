import json
import time
import requests
from urllib.parse import urlparse
from collections import Counter

WEBHOOK = "https://novoe-mesto.bitrix24.ru/rest/931/2e8tucb3bmkok4xt/"
UF_CODE = "UF_CRM_1738009865525"

def bx_list(method: str, params: dict) -> dict:
    url = WEBHOOK.rstrip("/") + f"/{method}.json"
    r = requests.post(url, json=params, timeout=40)
    r.raise_for_status()
    return r.json()

def to_host(v: str) -> str:
    if not v:
        return ""
    s = v.strip()
    # если пришло без схемы — добавим, чтобы urlparse корректно распарсил
    if not s.lower().startswith(("http://", "https://")):
        s = "https://" + s
    try:
        p = urlparse(s)
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        # для кириллицы Bitrix может отдавать уже в Unicode — это ок
        return host
    except Exception:
        return ""

def main():
    start = 0
    seen_rows = 0
    rows = []
    counts = Counter()

    print(f"Сканирую все сделки по {UF_CODE}...")
    while True:
        payload = {
            "select": ["ID", UF_CODE],
            "order": {"ID": "DESC"},
            "start": start
        }
        data = bx_list("crm.deal.list", payload)
        items = data.get("result") or []
        if not items:
            break

        for it in items:
            raw = it.get(UF_CODE)
            if raw in (None, "", []):
                continue
            host = to_host(raw if isinstance(raw, str) else str(raw))
            if not host:
                continue
            rows.append({"ID": it.get("ID"), "raw": raw, "host": host})
            counts[host] += 1

        seen_rows += len(items)
        nxt = data.get("next")
        if nxt is None:
            break
        start = nxt
        # не душим API
        time.sleep(0.12)

    print(f"\nОбработано строк: {seen_rows}")
    print(f"Уникальных хостов: {len(counts)}\n")

    print("ТОП-50 хостов по частоте:")
    for host, n in counts.most_common(50):
        print(f"{host}\t{n}")

    # Скелет для METRIKA_ROUTING_JSON
    skeleton = [
        {"uf_value": f"https://{host}", "counter_id": 0, "mp_token": ""}
        for host in sorted(counts.keys())
    ]

    with open("uf_hosts_counts.json", "w", encoding="utf-8") as f:
        json.dump(counts, f, ensure_ascii=False, indent=2)
    with open("metrika_routing_skeleton.json", "w", encoding="utf-8") as f:
        json.dump(skeleton, f, ensure_ascii=False, indent=2)

    print("\nФайлы сохранены:")
    print(" - uf_hosts_counts.json (частоты)")
    print(" - metrika_routing_skeleton.json (шаблон для .env)")

if __name__ == "__main__":
    main()
