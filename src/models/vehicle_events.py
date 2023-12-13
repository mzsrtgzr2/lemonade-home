from pydantic import BaseModel, Field
from typing import Optional

class VehicleEvent(BaseModel):
    vehicle_id: str
    event_time: str
    event_source: str
    event_type: str
    event_value: str
    note: str
    boot_time: Optional[int] = None
    emergency_call: Optional[bool] = None

    @classmethod
    def parse_raw(cls, data):
        if isinstance(data, str):
            data = super().parse_raw(data)
            if "event_extra_data" in data:
                extra_data = data.pop("event_extra_data")
                for key, value in extra_data.items():
                    data[key] = value
            return cls(**data)
        return super().parse_raw(data)