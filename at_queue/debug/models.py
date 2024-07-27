from pydantic import BaseModel
from typing import Dict

class ExecMetod(BaseModel):
    component: str
    method: str
    kwargs: Dict

class ExecMethodResult(BaseModel):
    result: Dict