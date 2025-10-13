from typing import Any, Dict, Optional


def safe_parse_json(value: Any) -> Optional[Dict[str, Any]]:
    try:
        if value is None:
            return None
        if isinstance(value, dict):
            return value  # already parsed
        import json
        return json.loads(value)
    except Exception:
        return None


