import re, hashlib

def normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    return re.sub(r"\D+", "", phone)

def sha256_hex(s: str) -> str:
    if not s:
        return ""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()
