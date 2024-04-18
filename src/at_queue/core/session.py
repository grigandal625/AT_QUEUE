import asyncio
import aio_pika
from yarl import URL
from ssl import SSLContext
from pamqp.common import FieldTable

from typing import Union, Dict, TypedDict, Callable, Optional, Type, Any, Awaitable, List
import logging
import json
import asyncio
from uuid import UUID, uuid4
import inspect

from at_queue.core import exceptions

logger = logging.getLogger(__name__)

class ConnectionKwargs(TypedDict):
    url: Union[str, URL, None]
    host: str
    port: int
    login: str
    password: str
    virtualhost: str
    ssl: bool
    loop: Optional[asyncio.AbstractEventLoop]
    ssl_options: Optional[aio_pika.abc.SSLOptions]
    ssl_context: Optional[SSLContext]
    timeout: aio_pika.abc.TimeoutType
    client_properties: Optional[FieldTable]
    connection_class: Type[aio_pika.abc.AbstractRobustConnection]


class ConnectionParameters:
    connection_kwargs: ConnectionKwargs

    def __init__(self, 
                 url: Union[str, URL, None] = None,
                 *,
                    host: str = "localhost",
                    port: int = 5672,
                    login: str = "guest",
                    password: str = "guest",
                    virtualhost: str = "/",
                    ssl: bool = False,
                    loop: Optional[asyncio.AbstractEventLoop] = None,
                    ssl_options: Optional[aio_pika.abc.SSLOptions] = None,
                    ssl_context: Optional[SSLContext] = None,
                    timeout: aio_pika.abc.TimeoutType = None,
                    client_properties: Optional[FieldTable] = None,
                    connection_class: Type[aio_pika.abc.AbstractRobustConnection] = aio_pika.RobustConnection,
                    **kwargs: Any
                ):
        self.connecion_kwargs = {
            'url': url,
            'host': host,
            'port': port,
            'login': login,
            'password': password,
            'virtualhost': virtualhost,
            'ssl': ssl,
            'loop': loop,
            'ssl_options': ssl_options,
            'ssl_context': ssl_context,
            'timeout': timeout,
            'client_properties': client_properties,
            'connection_class': connection_class,
            **kwargs
        }

    async def connect_robust(self) -> aio_pika.RobustConnection:
        return await aio_pika.connect_robust(**self.connecion_kwargs)


class BasicSession:
    connection_parameters: ConnectionParameters
    initialized: bool = False
    auto_ack: bool = True

    _send_connection: aio_pika.abc.AbstractConnection
    _send_channel: aio_pika.abc.AbstractChannel
    _send_queue: aio_pika.abc.AbstractQueue

    _recieve_connection: aio_pika.abc.AbstractConnection
    _recieve_channel: aio_pika.abc.AbstractChannel
    _recieve_queue: aio_pika.abc.AbstractQueue
    _listen_future: asyncio.Future

    uuid: UUID
    _process_message: Union[Callable, Awaitable] = None

    def __init__(self, connection_parameters: ConnectionParameters, uuid: Union[UUID, str] = None, auto_ack: bool = True, *args, **kwargs) -> None:
        if uuid is None:
            uuid = uuid4()
        if isinstance(uuid, str):
            uuid = UUID(uuid)
        self.uuid = uuid
        self.auto_ack = auto_ack
        self.connection_parameters = connection_parameters

    async def initialize(self) -> bool:
        self._send_connection = await self.connection_parameters.connect_robust()
        self._send_channel = await self._send_connection.channel()
        self._send_queue = await self._send_channel.declare_queue(f'send-{self.id}')

        self._recieve_connection = await self.connection_parameters.connect_robust()
        self._recieve_channel = await self._recieve_connection.channel()
        self._recieve_queue = await self._recieve_channel.declare_queue(f'recieve-{self.id}')
        
        self.initialized = True
        return self.initialized
    
    def _check_initialized(self):
        if not self.initialized:
            msg = f"Session with id '{self.id}' is not initialized"
            raise exceptions.SessionNotInitializedException(msg, self)
        
    @property
    def send_connection(self) -> aio_pika.abc.AbstractConnection:
        self._check_initialized()
        return self._send_connection
    
    @property
    def send_channel(self) -> aio_pika.abc.AbstractChannel:
        self._check_initialized()
        return self._send_channel
    
    @property
    def send_queue(self) -> aio_pika.abc.AbstractQueue:
        self._check_initialized()
        return self._send_queue
    
    @property
    def recieve_connection(self) -> aio_pika.abc.AbstractConnection:
        self._check_initialized()
        return self._recieve_connection
    
    @property
    def recieve_channel(self) -> aio_pika.abc.AbstractChannel:
        self._check_initialized()
        return self._recieve_channel
    
    @property
    def recieve_queue(self) -> aio_pika.abc.AbstractQueue:
        self._check_initialized()
        return self._recieve_queue

    @property
    def id(self):
        return str(self.uuid)
    
    async def _main_callback(self, message: aio_pika.IncomingMessage, *args, **kwargs):
        logger.info(f"Recieved message: {message.body.decode()}")
        if self.auto_ack:
            await message.ack()
        if self._process_message is not None:
            res = self._process_message(msg=message, session=self, *args, **kwargs)
            if inspect.iscoroutine(res):
                await res

    def process_message(self, func: Union[Callable, Awaitable]):
        self._process_message = func

    async def send(self, message: str) -> str:
        self._check_initialized()
        await self.send_channel.default_exchange.publish(
            aio_pika.Message(message.encode()), 
            routing_key=f'send-{self.id}'
        )
        return message
    
    async def listen(self):
        self._listen_future = asyncio.Future()
        self._check_initialized()
        logger.info(f"Started listening queue recieve-{self.id}")
        consumer = await self.recieve_queue.consume(self._main_callback)
        logger.info(f"Listening consumer: {consumer}")
        logger.info("To exit press CTRL+C")
        try:
            await self._listen_future
        except KeyboardInterrupt:
            self._listen_future.set_result(True)
            logger.info(f"Interrupted consumer: {consumer}")

    async def stop(self):
        await self.send_connection.close()
        await self.recieve_connection.close()
        self._listen_future.set_result(True)


class JSONSession(BasicSession):

    async def send(self, message: Dict) -> Dict:
        msg = json.dumps(message, ensure_ascii=False)
        await super().send(msg)
        return message
    
    def jsonify_body(self, func: Union[Callable, Awaitable]) -> Union[Callable, Awaitable]:
        def wrapper(msg: aio_pika.IncomingMessage, *args, **kwargs):
            try:
                body = msg.body.decode()
                body = json.loads(body)
                kwargs['session'] = self
                return func(
                    msg=msg,
                    body=body,
                    *args, 
                    **kwargs
                )
            except json.decoder.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON body {body}")
                raise e
        return wrapper

    def process_message(self, func: Union[Callable, Awaitable]):
        new_func = self.jsonify_body(func)
        return super().process_message(new_func)
    

class MessageBodyDict(TypedDict):
    message_id: str
    message: Union[Dict, None]
    sender: str
    reciever: str
    answer_to: Union[str, None]


class CommunicationSession(JSONSession):
    communicator_name: str
    warn_on_reciever_differs: bool = True

    def __init__(
        self, 
        communicator_name: str, 
        connection_parameters: ConnectionParameters, uuid: Union[UUID, str] = None,
        auto_ack: bool = True,
        warn_on_reciever_differs: bool = True,
        *args, **kwargs
    ) -> None:
        super().__init__(connection_parameters, uuid=uuid, auto_ack=auto_ack, *args, **kwargs)
        self.communicator_name = communicator_name
        self.warn_on_reciever_differs = warn_on_reciever_differs

    async def send(self, reciever: str, message: dict, answer_to: Optional[Union[UUID, str]] = None, message_id: Optional[Union[UUID, str]] = None, sender: str = None) -> MessageBodyDict:
        body = {
            'sender': sender or self.communicator_name, 
            'reciever': reciever, 
            'message': message,
            'message_id': str(message_id) if message_id else str(uuid4())
        }
        if answer_to is not None:
            body['answer_to'] = str(answer_to)
        else:
            body['answer_to'] = answer_to
        return await super().send(body)
    
    def communicate_body(self, func: Union[Callable, Awaitable]) -> Union[Callable, Awaitable]:

        def wrapper(body: MessageBodyDict, *args, **kwargs):
            sender = body.get('sender')
            reciever = body.get('reciever')
            message = body.get('message')
            message_id = body.get('message_id')
            answer_to = body.get('answer_to')
            
            if sender is None:
                logger.warning('Sender is not specified')
            if (reciever != self.communicator_name) and self.warn_on_reciever_differs:
                logger.warning(f'Reciever "{reciever}" is different from session communicator name "{self.communicator_name}"')
            if message is None:
                logger.warning('Message is not specified')
            if message_id is None:
                logger.warning('Message ID is not specified')
            
            kwargs['session'] = self

            return func(
                sender=sender, 
                reciever=reciever,
                message=message,
                message_id=message_id,
                answer_to=answer_to,
                *args, 
                **kwargs
            )
    
        return wrapper
    
    def process_message(self, func: Callable):
        new_func = self.communicate_body(func)
        return super().process_message(new_func)


class AwaitingMessageBodyDict(MessageBodyDict):
    callback: Optional[Union[Callable, Awaitable]]
    future: Optional[asyncio.Future]


class QueueSession(CommunicationSession):

    awaiting_messages: Dict[str, AwaitingMessageBodyDict]
    callback_tasks: List[asyncio.Task]

    def __init__(
        self, 
        communicator_name: str, 
        connection_parameters: ConnectionParameters, 
        uuid: Union[str, UUID] = None,
        auto_ack: bool = True,
        warn_on_reciever_differs: bool = True,
        *args, **kwargs
    ) -> None:
        super().__init__(communicator_name, connection_parameters, uuid=uuid, auto_ack=auto_ack, warn_on_reciever_differs=warn_on_reciever_differs)
        self.awaiting_messages = {}
        self.callback_tasks = []
        self.process_message(lambda *args, **kwargs: None)

    async def send(self, reciever: str, message: dict, answer_to: Optional[Union[UUID, str]] = None, await_answer: bool = True, callback: Union[Callable, Awaitable] = None, message_id: Union[str, UUID] = None, sender: str = None) -> AwaitingMessageBodyDict:
        msg = await super().send(reciever, message, answer_to, message_id=message_id, sender=sender)
        if await_answer:
            msg['callback'] = callback
            msg['future'] = asyncio.Future()
            self.awaiting_messages[str(msg['message_id'])] = msg
        return msg
    
    def awaitify_recieve(self, func: Union[Callable, Awaitable]) -> Union[Callable, Awaitable]:

        async def wrapper(*args, message: dict, answer_to: Union[str, UUID] = None, **kwargs):
            msg = self.awaiting_messages.pop(str(answer_to), None)
            if msg is not None:
                future = msg.get('future')
                if future is not None:
                    future.set_result(message)
                callback = msg.get('callback')
                if callback is not None:
                    res = callback(*args, answer_to=answer_to, message=message, **kwargs)
                    if inspect.iscoroutine(res):
                        self.callback_tasks.append(res)
            res = func(*args, message=message, answer_to=answer_to, **kwargs)
            if inspect.iscoroutine(res):
                return await res
            return res
        
        return wrapper
    
    def process_message(self, func: Union[Callable, Awaitable]):
        new_func = self.awaitify_recieve(func)
        return super().process_message(new_func)
    
    async def listen(self):
        result = await super().listen()
        await asyncio.gather(*self.callback_tasks)
        return result
        

class MSGAwaitSession(QueueSession):

    def __init__(self, communicator_name: str, connection_parameters: ConnectionParameters, uuid: str | UUID = None, auto_ack: bool = True, warn_on_reciever_differs: bool = True, *args, **kwargs) -> None:
        super().__init__(communicator_name, connection_parameters, uuid, auto_ack=auto_ack, warn_on_reciever_differs=warn_on_reciever_differs, *args, **kwargs)
        self.process_message(lambda *args, **kwargs: None)

    async def send_await(self, reciever: str, message: dict, answer_to: Optional[Union[UUID, str]] = None, await_answer: bool = True, callback: Callable[..., Any] = None):
        message: AwaitingMessageBodyDict = await self.send(reciever, message, answer_to, await_answer, callback)
        self.awaiting_messages[str(message['message_id'])] = message
        future = message.get('future')
        if future is not None:
            await future
            return future.result()
        return None


class ReversedSession(BasicSession):

    async def send(self, message: str) -> str:
        msg = f'Reversed Session {self.id} will send message {message} to recieve queue recieve-{self.id}'
        logger.warning(msg)
        return await self.recieve_queue_send(message)

    async def listen(self):
        msg = f'Reversed Session {self.id} will listen send queue send-{self.id}'
        logger.warning(msg)
        return await self.send_queue_listen()
    
    async def recieve_queue_send(self, message: str) -> str:
        self._check_initialized()
        await self.recieve_channel.default_exchange.publish(
            aio_pika.Message(message.encode()), 
            routing_key=f'recieve-{self.id}'
        )
        return message
    
    async def _send_callback(self, message: aio_pika.IncomingMessage, *args, **kwargs):
        logger.info(f"Recieved message (in send queue): {message.body.decode()}")
        if self.auto_ack:
            await message.ack()
        if self._process_message is not None:
            res = self._process_message(msg=message, session=self, *args, **kwargs)
            if inspect.iscoroutine(res):
                await res

    async def send_queue_listen(self):
        self._listen_future = asyncio.Future()
        self._check_initialized()
        logger.info(f"Started reversed session listening queue send-{self.id}")
        consumer = await self.send_queue.consume(self._send_callback)
        logger.info(f"Listening consumer: {consumer}")
        logger.info("To exit press CTRL+C")
        try:
            await self._listen_future
        except KeyboardInterrupt:
            self._listen_future.set_result(True)
            logger.info(f"Interrupted consumer: {consumer}")


class JSONReversedSession(ReversedSession, JSONSession):
    
    async def send(self, message: dict) -> str:
        msg = json.dumps(message, ensure_ascii=False)
        await ReversedSession.send(self, msg)
        return message
    

class CommunicationReversedSession(JSONReversedSession, CommunicationSession):

    def __init__(self, communicator_name: str, connection_parameters: ConnectionParameters, uuid: UUID | str = None, auto_ack: bool = True, warn_on_reciever_differs: bool = True, *args, **kwargs) -> None:
        super().__init__(communicator_name, connection_parameters, uuid, auto_ack, *args, **kwargs)
        self.warn_on_reciever_differs = warn_on_reciever_differs

    async def send(self, reciever: str, message: dict, answer_to: Optional[Union[UUID, str]] = None, message_id: Optional[Union[UUID, str]] = None, sender: str = None, warn_on_reciever_differs: bool = True, *args, **kwargs) -> MessageBodyDict:
        body = {
            'sender': sender or self.communicator_name, 
            'reciever': reciever, 
            'message': message,
            'message_id': str(message_id) if message_id else str(uuid4())
        }
        if answer_to is not None:
            body['answer_to'] = str(answer_to)
        else:
            body['answer_to'] = answer_to
        return await JSONReversedSession.send(self, body)