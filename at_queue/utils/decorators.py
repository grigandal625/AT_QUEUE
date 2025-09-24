from typing import Awaitable
from typing import Callable
from typing import Union

from at_queue.core.at_component import ATComponentMethod
from at_queue.core.at_component import AuthorizedATComponentMethod


def component_method(method: Union[Callable, Awaitable]):
    return ATComponentMethod(method)


def authorized_method(method: Union[Callable, Awaitable]):
    return AuthorizedATComponentMethod(method)
