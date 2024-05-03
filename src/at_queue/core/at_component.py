from at_queue.utils.schema_inspector import method_to_json_schema, return_type_to_json_schema
from dataclasses import dataclass
from jsonschema import validate, SchemaError
from typing import Any, Dict, Callable, Union
from collections import OrderedDict
import inspect
from at_queue.core.session import MSGAwaitSession, ConnectionParameters
from uuid import UUID, uuid4, uuid3, NAMESPACE_OID
from at_queue.core import exceptions
from jsonschema import validate, SchemaError, ValidationError
import asyncio
import logging
import aio_pika
import traceback
import json
from at_config.core.at_config_handler import ATComponentConfig
from at_config.core.at_config_loader import load_component_config


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
        except ValidationError as e:
            if raise_exception:
                raise e
            return False
        
    @property
    def __dict__(self):
        return {
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
        return {'name': self.name, 'required': self.required, **(super().__dict__)}


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
    owner: 'ATComponent' = None
    metadata: Dict

    def __init__(self, orig: Callable) -> None:
        self.metadata = {}
        self.orig = orig
        if orig.__name__ == '<lambda>':
            raise TypeError("Can't create a component method for anonymous function")
        
        name = orig.__name__
        inputs = self.get_inputs()
        output = self.get_output()
        super(ATComponentMethod, self).__init__(name=name, inputs=inputs, output=output)

    def _check_owner(self):
        if not self.owner:
            raise ValueError(f'Missing ATComponent instance to call "{self.name}"')
        return True

    def __call__(self, *args, **kwargs):
        self._check_owner()
        return self.orig(self.owner, *args, **kwargs)
    
    def execute(self, *, processed_message: Dict, processed_message_id: Union[str, UUID], exec_arguments: Dict):
        self.metadata['processed_message'] = processed_message
        self.metadata['processed_message_id'] = processed_message_id
        return self.__call__(**exec_arguments)

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
    

class AuthorizedATComponentMethod(ATComponentMethod):
    def __call__(self, *args, **kwargs):
        self._check_owner()
        if 'auth_token' not in kwargs:
            raise exceptions.NotAuthorizedMethodException(
                f'Missing auth_token argument to execute method {self.owner.name}.{self.name}', 
                session=self.owner.session, 
                component=self.owner, 
                processed_message=self.metadata.get('processed_message', None),
                processed_message_id=self.metadata.get('processed_message_id', None)
            )
        return super().__call__(*args, **kwargs)

    def get_inputs(self) -> Dict[str, Input]:
        res = super().get_inputs()
        res.pop('auth_token', None)
        return res


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

class ATComponentMetaClass(type):
    
    @property
    def _at_methods_names(self):
        return [member.name for _, member in inspect.getmembers(self) if isinstance(member, ATComponentMethod)]


class ATComponent(BaseComponent, metaclass=ATComponentMetaClass):
    session: MSGAwaitSession
    register_session: MSGAwaitSession
    registered: bool = False
    register_msg_id: Union[str, UUID]
    register_future: asyncio.Future
    initialized: bool = False
    register_listen_task: asyncio.Task

    def __init__(self, connection_parameters: ConnectionParameters, *args, **kwargs):
        kwargs['name'] = kwargs.get('name', self.__class__.__name__)
        kwargs['methods'] = self._methods
        super().__init__(*args, **kwargs)
        self.register_future = asyncio.Future()
        self.register_session = MSGAwaitSession(self.name, connection_parameters, uuid3(NAMESPACE_OID, 'at_registry'), auto_ack=False, warn_on_reciever_differs=False)

    @property
    def _methods(self):
        res = {}
        for method_name in self.__class__._at_methods_names:
            method = getattr(self, method_name)
            method.owner = self
            res[method_name] = method
        return res

    async def initialize(self):
        await self.register_session.initialize()
        self.register_session.process_message(self._register_callback)
        self.initialized = True

    def _check_initialized(self):
        if not self.initialized:
            raise exceptions.ComponentNotInitializedException(f'Component {self.name} is not initialized', self.session, self)

    async def register(self):
        self._check_initialized()
        message = {
            'type': 'register', 
            'component': self.__dict__
        }
        self.register_listen_task = asyncio.get_event_loop().create_task(self.register_session.listen())
        message = await self.register_session.send('registry', message)
        self.register_msg_id = message.get('message_id')
        await self.register_future
        self.registered = self.register_future.result()
        if not self.registered:
            self.register_msg_id = uuid4()
            self.register_future = asyncio.Future()
        else:
            logger.info(f'Component "{self.name}" registered successfully')
            await self.register_session.stop()
            await self.register_listen_task

    async def _register_callback(self, *args, message: dict, sender: str, reciever: str, message_id: str, answer_to: str, errors: Any = None, msg: aio_pika.IncomingMessage, **kwargs) -> bool:
        if not self.registered and (reciever == self.name):
            logger.info(f'Register component callback received message with id {message_id} about registering: {message}') 
            if sender != 'registry':
                logger.warning(f'Register component callback received message not from registry, but from "{sender}"')
            if str(answer_to) == str(self.register_msg_id):
                if errors is not None:
                    logger.error(errors)
                    msg = f'Got errors while registering component "{self.name}": {errors}'
                    raise exceptions.RegisterException(msg, self.session, self, message_id, message)
                await msg.ack()
                
                session_id = message.get('session_id')
                self.session = MSGAwaitSession(self.name, self.register_session.connection_parameters, uuid=session_id)
                await self.session.initialize()
                self.session.process_message(self._log_process_message)
                
                self.register_future.set_result(True)
                return True
            return False

    async def _log_process_message(self, *args, message: dict, sender: str, reciever: str, message_id: str, **kwargs):
        logger.info(f'Start for "{self.name}" processing message {message_id}: {message} from "{sender}" with arguments: {args}, {kwargs}')
        if reciever != self.name:
            logger.warning(f'Component "{self.name}" received message that is not for "{self.name}", but for "{reciever}"')

        await self._process_message(self, message=message, sender=sender, reciever=reciever, message_id=message_id, *args, **kwargs)

        logger.info(f'Finish for "{self.name}" processing message {message_id}')

    async def _process_message(self, *args, message: dict, sender: str, reciever: str, message_id: str, msg: aio_pika.IncomingMessage, **kwargs):
        message_type = message.get('type')
        try:
            if message_type == 'ping':
                result = await self.session.send(reciever=sender, message={'result': 'pong'}, answer_to=message_id, **msg.headers)
            if message_type == 'configurate':
                result = await self._configurate(*args,  message=message, sender=sender, message_id=message_id, reciever=reciever, msg=msg, **kwargs)
            if message_type == 'check_configured':
                configurated = await self._is_configured(*args, message=message, sender=sender, message_id=message_id, reciever=reciever, msg=msg, **kwargs)
                result = await self.session.send(reciever=sender, message={'result': configurated}, answer_to=message_id, **msg.headers)
            if message_type == 'exec_method':
                result = await self._exec_method(*args,  message=message, sender=sender, message_id=message_id, reciever=reciever, msg=msg, **kwargs)
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())

    async def _is_configured(self, *args, message: dict, sender: str, message_id: Union[str, UUID], reciever: str, msg: aio_pika.IncomingMessage, **kwargs) -> bool:
        auth_token = msg.headers.get('auth_token')
        return await self.check_configured(*args, message=message, sender=sender, message_id=message_id, reciever=reciever, msg=msg, auth_token=auth_token, **kwargs)

    async def check_configured(self, *args, message: dict, sender: str, message_id: Union[str, UUID], reciever: str, msg: aio_pika.IncomingMessage, auth_token: str = None, **kwargs) -> bool:
        return True

    async def _configurate(self, *args, message: dict, sender: str, message_id: Union[str, UUID], msg: aio_pika.IncomingMessage, **kwargs):
        config_data = message.get('config')
        if config_data is None:
            msg = f'Component "{self.name}" received configurate message with id "{message_id}" from "{sender}" but configuration is not specified'
            e = exceptions.ProcessMessageException(msg, self.session, self, message)
            logger.error(e.__dict__)
            await self.session.send(reciever=sender, message={'errors': [e.__dict__]}, answer_to=message_id, await_answer=False)
            asyncio.sleep(0)
            raise e
        
        auth_token = msg.headers.get('auth_token', None)
        config = load_component_config(config_data)
        result = await self.perform_configurate(config, auth_token=auth_token)
        await self.session.send(reciever=sender, message={'result': result}, answer_to=message_id, await_answer=False)

    async def perform_configurate(self, config: ATComponentConfig, auth_token: str = None, *args, **kwargs) -> bool:
        return True
            
    async def _exec_method(self, *args, message: dict, sender: str, message_id: Union[str, UUID], msg: aio_pika.IncomingMessage, **kwargs):
        method_name = message.get('method')
        if method_name is None:
            msg = f'Component "{self.name}" received execute method message with id "{message_id}" from "{sender}" but method name is not specified'
            e = exceptions.ProcessMessageException(msg, self.session, self, message)
            logger.error(e.__dict__)
            self.session.send(reciever=sender, message={'errors': [e.__dict__]}, answer_to=message_id, await_answer=False)
            raise e
        
        method: ATComponentMethod = self.methods.get(method_name)

        if method is None:
            msg = f'Component "{self.name}" received execute method message with id "{message_id}" from "{sender}" but got unknown method name "{method_name}". Avalible method names are: {[m for m in self.methods]}'
            e = exceptions.ExecMethodException(msg, self.session, self, message)
            logger.error(e.__dict__)
            await self.session.send(reciever=sender, message={'errors': [e.__dict__]}, answer_to=message_id, await_answer=False)
            asyncio.sleep(0)
            raise e
        
        method_args = message.get('args', {})
        exec_kwargs = {}
        if isinstance(method, AuthorizedATComponentMethod):
            auth_token = msg.headers.get('auth_token', None)
            if auth_token is not None:
                exec_kwargs['auth_token'] = auth_token
        for input_name, input in method.inputs.items():
            arg = method_args.get(input_name)
            try:
                validate(arg, input.schema)
            except SchemaError as s:
                msg = str(s)
                e = exceptions.MethodArgumentSchemaException(msg, self.session, self, message_id, message_id, method, input)
                await self.session.send(reciever=sender, message={'errors': [e.__dict__]}, answer_to=message_id, await_answer=False)
                asyncio.sleep(0)
                raise e
            except ValidationError as s:
                msg = str(s)
                e = exceptions.MethodArgumentSchemaException(msg, self.session, self, message_id, message_id, method, input)
                await self.session.send(reciever=sender, message={'errors': [e.__dict__]}, answer_to=message_id, await_answer=False)
                asyncio.sleep(0)
                raise e                
            exec_kwargs[input_name] = arg

        try:
            result = method.execute(processed_message=message, processed_message_id=message_id, exec_arguments=exec_kwargs)
            if inspect.iscoroutine(result):
                result = await result
            await self.session.send(reciever=sender, message={'type': 'method_result', 'result': result}, answer_to=message_id, await_answer=False)
            return result
        except exceptions.ExecMethodException as e:
            logger.error(traceback.format_exc())
            await self.session.send(reciever=sender, message={'errors': [e.__dict__]}, answer_to=message_id, await_answer=False)
            return
        except Exception as e:
            logger.error(traceback.format_exc())
            await self.session.send(reciever=sender, message={'errors': [str(e)]}, answer_to=message_id, await_answer=False)
            return
    
    async def exec_external_method(self, reciever: str, methode_name: str, method_args: dict, auth_token: str = None) -> dict:
        self.session._check_initialized()
        headers = {}
        if auth_token is not None:
            headers['auth_token'] = auth_token
        exec_result = await self.session.send_await(
            reciever=reciever, 
            message={
                'type': 'exec_method',
                'method': methode_name,
                'args': method_args
            },
            **headers
        )
        errors = exec_result.get('errors')
        if errors is not None:
            msg = f'''Got errors while executing external method {reciever}.{methode_name}:

{json.dumps(errors, indent=4)}'''
            logger.error(msg)
            raise exceptions.ExternalMethodException(msg, self.session, self, exec_result.get('message_id'), exec_result)
        if exec_result.get('type') != 'method_result':
            msg_type = exec_result.get('type')
            logger.warning(f'Recieved unexpected message type "{msg_type}". Expected: "method_result"')
        return exec_result.get('result')
    
    async def ping_external(self, reciever: str) -> bool:
        result = await self.session.send_await(reciever, {'type': 'ping'})
        if result.get('result') == 'pong':
            return True
        return False
    
    async def check_external_configured(self, reciever: str, auth_token: str = None) -> bool:
        result = await self.session.send_await(reciever, {'type': 'check_configured'}, auth_token=auth_token)
        if result.get('result'):
            return True
        return False
    
    async def check_external_registered(self, reciever: str) -> bool:
        result = await self.session.send_await('registry', {'type': 'check_registered', 'component': reciever})
        if result.get('result'):
            return True
        return False

    async def start(self):
        self.session._check_initialized()
        await self.session.listen()