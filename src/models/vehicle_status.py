from pydantic import BaseModel

class VehicleStatus(BaseModel):
    vehicle_id: str
    report_time: str
    status_source: str
    status: str
