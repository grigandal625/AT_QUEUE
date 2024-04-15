from at_queue.core.at_component import ATComponentMethod
from typing import Callable, Awaitable, Union

def component_method(method: Union[Callable, Awaitable]):
    return ATComponentMethod(method)
