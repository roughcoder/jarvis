from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
