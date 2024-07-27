from pydantic import BaseModel
from typing import Dict, Any

class ExecMetod(BaseModel):
    component: str
    method: str
    kwargs: Dict
    auth_token: str = None

class ExecMethodResult(BaseModel):
    result: Any