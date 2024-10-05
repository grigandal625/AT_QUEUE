from pydantic import BaseModel, Field
from typing import Dict, Any, Optional

class ExecMetod(BaseModel):
    component: str
    method: str
    kwargs: Dict
    auth_token: Optional[str] = Field(None)

class ExecMethodResult(BaseModel):
    result: Any