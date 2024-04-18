from typing import TYPE_CHECKING, Union
from uuid import UUID
from collections import OrderedDict

if TYPE_CHECKING:
    from at_queue.core.session import BasicSession
    from at_queue.core.at_component import BaseComponent, BaseComponentMethod, Input


class ATQueueException(Exception):
    msg: str

    def __init__(self, msg: str, *args: object) -> None:
        super().__init__(msg, *args)
        self.msg = msg

    @property
    def __dict__(self):
        result = OrderedDict()
        result['error_message'] = self.msg
        result['error_type'] = self.__class__.__name__
        return result
    
    def __str__(self) -> str:
        return self.msg


class SessionException(ATQueueException):
    session: 'BasicSession'
    
    def __init__(self, msg: str, session: 'BasicSession', *args) -> None:
        super().__init__(msg, session, *args)
        self.session = session

    @property
    def __dict__(self):
        result = super().__dict__
        result['session'] = self.session.id
        return result
    
    
class SessionNotInitializedException(SessionException):
    pass


class ATComponentException(SessionException):
    component: 'BaseComponent'

    def __init__(self, msg: str, session: 'BasicSession', component: 'BaseComponent', *args) -> None:
        super().__init__(msg, session, component, *args)
        self.component = component

    @property
    def __dict__(self):
        result = super().__dict__
        result['component'] = self.component.name
        return result


class ComponentNotInitializedException(ATComponentException):
    pass


class ProcessMessageException(ATComponentException):
    processed_message: dict
    processed_message_id: Union[UUID, str]

    def __init__(self, msg: str, session: 'BasicSession', component: 'BaseComponent', processed_message_id: Union[UUID, str], processed_message: dict, *args) -> None:
        super().__init__(msg, session, component, processed_message_id, processed_message, *args)
        self.processed_message = processed_message
        self.processed_message_id = processed_message_id

    @property
    def __dict__(self):
        result = super().__dict__
        result['message_id'] = self.processed_message_id
        result['message'] = self.processed_message
        return result
    
class RegisterException(ProcessMessageException):
    pass

class ExternalMethodException(ProcessMessageException):
    pass

class ExecMethodException(ProcessMessageException):
    method: 'BaseComponentMethod'

    def __init__(self, msg: str, session: 'BasicSession', component: 'BaseComponent', processed_message_id: UUID | str, processed_message: dict, *args, method: 'BaseComponentMethod' = None) -> None:
        super().__init__(msg, session, component, processed_message_id, processed_message, *args, method)
        self.method = method

    @property
    def __dict__(self):
        result = super().__dict__
        result['method'] = self.method.name if self.method is not None else None
        return result

class MethodArgumentSchemaException(ExecMethodException):
    argument: 'Input'

    def __init__(self, msg: str, session: 'BasicSession', component: 'BaseComponent', processed_message_id: UUID | str, processed_message: dict, method: 'BaseComponentMethod', argument: 'Input', *args) -> None:
        super().__init__(msg, session, component, processed_message_id, processed_message, argument, *args, method=method)
        self.argument = argument

    @property
    def __dict__(self):
        result = super().__dict__
        result['argument'] = self.argument.name
        return result
    