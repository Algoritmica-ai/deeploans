from pydantic import BaseModel, Field


class Table(BaseModel):
    field_number: str
    priority: str
    tag: str
    is_api_filter: bool
    field_name: str
    data_type: str
    definition: str
    max_length: str
    sample: str
    notes: str
