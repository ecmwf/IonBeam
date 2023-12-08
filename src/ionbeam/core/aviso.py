import requests
from typing import Any
from datetime import datetime


def send_aviso_notification(request: dict[str, Any]):
    notification = {
        "type": "aviso",
        "data": {"event": "iot-data", "request": request},
        "datacontenttype": "application/json",
        "id": "0c02fdc5-148c-43b5-b2fa-cb1f590369ff",
        "source": "/host/user",
        "specversion": "1.0",
        "time": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
    }

    response = requests.post("https://iot-notifications.ecmwf.int/api/v1/notification", json=notification)
    # response.raise_for_status()
    return response
