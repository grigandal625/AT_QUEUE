from at_queue.utils.schema_inspector import method_to_json_schema, return_type_to_json_schema
from dataclasses import dataclass
from jsonschema import validate, SchemaError
from typing import Any, Dict, Callable, Union
from collections import OrderedDict
import inspect
from at_queue.core.session import MSGAwaitSession, ConnectionParameters
from uuid import UUID
import logging
from at_queue.core import exceptions
from jsonschema import validate, SchemaError


logger = logging.getLogger(__name__)


@dataclass(kw_only=True)
class Parameter:
    schema: dict
    strict_validation: bool = False
    description: str = None
    method: 'ATComponentMethod' = None

    def validate(self, value: Any, raise_exception: bool = False) -> bool:
        try:
            validate(value, self.schema)
            return True
        except SchemaError as e:
            if raise_exception:
                raise e
            return False
        
    @property
    def __dict__(self):
        return {
            'name': self.name,
            'schema': self.schema,
            'strict_validation': self.strict_validation,
            'description': self.description
        }
    

@dataclass(kw_only=True)
class Input(Parameter):
    name: str
    required: bool = False

    @property
    def __dict__(self):
        return dict(required=self.required, **(super().__dict__))


@dataclass(kw_only=True)
class Output(Parameter):
    pass


@dataclass(kw_only=True)
class BaseComponentMethod:
    name: str
    description: str = None
    inputs: Dict[str, Input]
    output: Output

    @property
    def __dict__(self):
        return {
            'name': self.name,
            'description': self.description,
            'inputs': {
                input_name: input.__dict__
                for input_name, input in self.inputs.items()
            },
            'output': self.output.__dict__
        }


class ATComponentMethod(BaseComponentMethod):
    orig: Callable

    def __init__(self, orig: Callable) -> None:

        self.orig = orig
        if orig.__name__ == '<lambda>':
            raise TypeError("Can't create a component method for anonymous function")
        
        name = orig.__name__
        inputs = self.get_inputs()
        output = self.get_output()
        super(ATComponentMethod, self).__init__(name=name, inputs=inputs, output=output)

    def __call__(self, *args, **kwargs):
        return self.orig(*args, **kwargs)
    
    @property
    def args_schema(self) -> dict:
        return method_to_json_schema(self.orig)
    
    def get_inputs(self) -> Dict[str, Input]:
        result = OrderedDict()
        args_schema = self.args_schema
        for name, schema in args_schema['properties'].items():
            result[name] = Input(
                name=name,
                schema=schema, 
                required=name in args_schema.get('required', []),
                method=self
            )
        return result
    
    @property
    def return_schema(self) -> dict:
        return return_type_to_json_schema(self.orig)

    def get_output(self) -> Output:
        return Output(
            schema=self.return_schema,
            method=self
        )
    

@dataclass(kw_only=True)
class BaseComponent:
    name: str
    description: str = None
    methods: Dict[str, BaseComponentMethod]

    @property
    def __dict__(self):
        return {
            'name': self.name,
            'description': self.description,
            'methods': {
                method_name: method.__dict__
                for method_name, method in self.methods.items()
            }
        }


class ATComponent(BaseComponent):
    session: MSGAwaitSession
    registered: False

    def __new__(cls, name: str = None, description: str = None, all_as_methods: bool = False, *args, **kwargs) -> 'ATComponent':
        name = name or cls.__name__
        methods = OrderedDict()
        for member in inspect.getmembers(cls):
            if isinstance(member, ATComponentMethod):
                methods[member.name] = member
            elif callable(member) and all_as_methods:
                method = ATComponentMethod(member)
                methods[method.name] = method

        return super().__new__(cls, name=name, description=description, methods=methods, *args, **kwargs)
    
    def __init__(self, connection_parameters: ConnectionParameters, session_id: Union[str, UUID], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = MSGAwaitSession(self.name, connection_parameters, uuid=session_id)

    async def register(self):
        message = {'type': 'register', 'component': self.__dict__}
        await self.session.send('registry', message, await_answer=True, callback=self.register_callback)

    def register_callback(self, *args, message_id: Union[UUID, str], message: dict, sender: str, reciever: str, **kwargs):
        errors = message.get('errors')
        if sender != 'registry':
            logger.warning(f'Register component callback received message not from registry, but from "{sender}"')
        if reciever != self.name:
            logger.warning(f'Register component callback received message that is not for "{self.name}", but for "{reciever}"')
        if errors is not None:
            logger.error(errors)
            msg = f'Got errors while registering component "{self.name}": {errors}'
            raise exceptions.RegisterException(msg, self.session, self, message_id, message)
        self.registered = True
        logger.info(f'Component "{self.name}" registered successfully')
        self.session.process_message(self._log_process_message)

    def _log_process_message(self, *args, message: dict, sender: str, reciever: str, meesage_id: str, **kwargs):
        logger.info(f'Start for "{self.name}" processing message {meesage_id}: {message} from "{sender}" with arguments: {args}, {kwargs}')
        if reciever != self.name:
            logger.warning(f'Component "{self.name}" received message that is not for "{self.name}", but for "{reciever}"')

        self._process_message(self, message=message, sender=sender, reciever=reciever, meesage_id=meesage_id, *args, **kwargs)

        logger.info(f'Finish for "{self.name}" processing message {meesage_id}')

    def _process_message(self, *args, message: dict, sender: str, reciever: str, message_id: str, **kwargs):
        message_type = message.get('type')
        if message_type == 'exec_method':
            self._exec_method(*args,  message=message, sender=sender, message_id=message_id, reciever=reciever, **kwargs)
    
    async def _exec_method(self, *args, message, sender, message_id, **kwargs):
        method_name = message.get('method')
        if method_name is None:
            msg = f'Component "{self.name}" received execute method message with id "{message_id}" from "{sender}" but method name is not specified'
            e = exceptions.ProcessMessageException(msg, self.session, self, message)
            logger.error(e.__dict__)
            self.session.send(reciever=sender, message={'errors': [e.__dict__]}, answer_to=message_id, await_answer=False)

        method = self.methods.get(method_name)

        if method is None:
            msg = f'Component "{self.name}" received execute method message with id "{message_id}" from "{sender}" but got unknown method name "{method_name}". Avalible method names are: {[m for m in self.methods]}'
            e = exceptions.ExecMethodException(msg, self.session, self, message)
            logger.error(e.__dict__)
            self.session.send(reciever=sender, message={'errors': [e.__dict__]}, answer_to=message_id, await_answer=False)

        method_args = message.get('args', {})
        exec_kwargs = {}
        for input_name, input in method.inputs.items():
            arg = method_args.get(input_name)
            try:
                validate(arg, input.schema)
            except SchemaError as s:
                msg = str(s)
                e = exceptions.MethodArgumentSchemaException(msg, self.session, self, message_id, message_id, method, input)
                self.session.send(reciever=sender, message={'errors': [e.__dict__]}, answer_to=message_id, await_answer=False)
                raise e
            exec_kwargs[input_name] = arg

        result = method(**exec_kwargs)
        await self.session.send(reciever=sender, message={'type': 'method_result', 'result': result}, answer_to=message_id, await_answer=False)
        return result
    
    async def exec_external_method(self, reciever: str, methode_name: str, method_args: dict) -> dict:
        exec_result = await self.session.send_await(
            reciever=reciever, 
            message={
                'type': 'exec_method',
                'method': methode_name,
                'args': method_args
            },
        )
        errors = exec_result.get('errors')
        if errors is not None:
            logger.error(errors)
            msg = f'Got errors while registering component "{self.name}": {errors}'
            raise exceptions.ExternalMethodException(msg, self.session, self, exec_result.get('message_id'), exec_result)
        if exec_result.get('type') != 'method_result':
            msg_type = exec_result.get('type')
            logger.warning(f'Recieved unexpected message type "{msg_type}". Expected: "method_result"')
        return exec_result.get('result')
    
    async def start(self):
        await self.session.listen()