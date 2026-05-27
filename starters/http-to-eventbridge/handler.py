from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def build_eventbridge_entry(request: dict[str, Any]) -> dict[str, Any]:
    order_id = request["orderId"]
    source = request["source"]
    payload = request.get("payload", {})

    return {
        "DetailType": "order.received",
        "Source": f"tcs.integration.{source}",
        "Time": datetime.now(timezone.utc).isoformat(),
        "Detail": json.dumps(
            {
                "orderId": order_id,
                "payload": payload,
            }
        ),
    }
