from typing import Any
from typing import Dict
from typing import Optional

from pydantic import BaseModel
from pydantic import Field


class ExecMetod(BaseModel):
    component: str
    method: str
    kwargs: Dict
    auth_token: Optional[str] = Field(None)


class ExecMethodResult(BaseModel):
    result: Any
