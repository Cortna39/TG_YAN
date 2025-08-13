import time
from app.db import conn, get_routing_map
from app.settings import settings
from app.logger import get_logger

log = get_logger("app.router")

class Router:
    def __init__(self):
        self._cache = {}
        self._loaded_at = 0

    def refresh(self):
        with conn() as c:
            dbm = get_routing_map(c)
        envm = {}
        for item in settings.metrika_routing_json:
            envm[str(item["uf_value"]).strip().lower()] = {
                "counter_id": int(item["counter_id"]),
                "mp_token": item["mp_token"]
            }
        dbm.update(envm)
        self._cache = dbm
        self._loaded_at = time.time()
        log.info("routing_refreshed", extra={"count": len(self._cache)})

    def pick(self, uf_value: str):
        if time.time() - self._loaded_at > 300:
            self.refresh()
        route = self._cache.get((uf_value or "").strip().lower())
        if not route:
            log.warning("routing_miss", extra={"uf_value": uf_value})
        return route

router = Router()
