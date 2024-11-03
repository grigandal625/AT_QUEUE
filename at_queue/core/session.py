import asyncio
import aio_pika
from yarl import URL
from ssl import SSLContext
from pamqp.common import FieldTable

from typing import Union, Dict, TypedDict, Callable, Optional, Type, Any, Awaitable, List
import logging
import json
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
        """
        Initialize a new instance of the ConnectionParameters class.

        Parameters:
        url (Union[str, URL, None], optional): The URL of the RabbitMQ server. Defaults to None.
        host (str, optional): The host of the RabbitMQ server. Defaults to "localhost".
        port (int, optional): The port of the RabbitMQ server. Defaults to 5672.
        login (str, optional): The login username for the RabbitMQ server. Defaults to "guest".
        password (str, optional): The password for the RabbitMQ server. Defaults to "guest".
        virtualhost (str, optional): The virtual host for the RabbitMQ server. Defaults to "/".
        ssl (bool, optional): Whether to use SSL for the connection. Defaults to False.
        loop (Optional[asyncio.AbstractEventLoop], optional): The event loop to use for the connection. Defaults to None.
        ssl_options (Optional[aio_pika.abc.SSLOptions], optional): The SSL options for the connection. Defaults to None.
        ssl_context (Optional[SSLContext], optional): The SSL context for the connection. Defaults to None.
        timeout (aio_pika.abc.TimeoutType, optional): The timeout for the connection. Defaults to None.
        client_properties (Optional[FieldTable], optional): The client properties for the connection. Defaults to None.
        connection_class (Type[aio_pika.abc.AbstractRobustConnection], optional): The connection class to use. Defaults to aio_pika.RobustConnection.
        **kwargs (Any, optional): Additional keyword arguments.

        Returns:
        None
        """
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
    _started_future: asyncio.Future

    uuid: UUID
    _process_message: Union[Callable, Awaitable] = None

    def __init__(self, connection_parameters: ConnectionParameters, uuid: Union[UUID, str] = None, auto_ack: bool = True, *args, **kwargs) -> None:
        """
        Creates a new instance of the Session class.

        Parameters:
        - connection_parameters (ConnectionParameters): The connection parameters for the session.
        - uuid (Union[UUID, str], optional): The unique identifier for the session. If not provided, a new UUID will be generated.
        - auto_ack (bool, optional): Whether to automatically acknowledge received messages. Default is True.

        Returns:
        - None
        """
        if uuid is None:
            uuid = uuid4()
        if isinstance(uuid, str):
            uuid = UUID(uuid)
        self.uuid = uuid
        self.auto_ack = auto_ack
        self.connection_parameters = connection_parameters
        self._started_future = asyncio.get_event_loop().create_future()

    
    async def initialize(self) -> bool:
        """
        Initializes the session by creating and declaring send and receive queues.

        Parameters:
        - self: The current instance of the Session class.

        Returns:
        - bool: True if the session is successfully initialized, False otherwise.

        Raises:
        - None
        """
        self._send_connection = await self.connection_parameters.connect_robust()
        self._send_channel = await self._send_connection.channel()
        self._send_queue = await self._send_channel.declare_queue(f'send-{self.id}')

        self._recieve_connection = await self.connection_parameters.connect_robust()
        self._recieve_channel = await self._recieve_connection.channel()
        self._recieve_queue = await self._recieve_channel.declare_queue(f'recieve-{self.id}')

        self.initialized = True
        return self.initialized
    
    def _check_initialized(self):
        """
        Checks if the session is initialized.

        Raises:
        SessionNotInitializedException: If the session is not initialized.

        The exception includes the session's ID and a reference to the session instance.
        """
        if not self.initialized:
            msg = f"Session with id '{self.id}' is not initialized"
            raise exceptions.SessionNotInitializedException(msg, self)
        
    @property
    def send_connection(self) -> aio_pika.abc.AbstractConnection:
        """
        Returns the connection for sending messages within the session.

        The send connection is used to establish a channel for sending messages to the send queue.

        Parameters:
        None

        Returns:
        aio_pika.abc.AbstractConnection: The send connection for the session.

        Raises:
        SessionNotInitializedException: If the session is not initialized.
        """
        self._check_initialized()
        return self._send_connection
    
    @property
    def send_channel(self) -> aio_pika.abc.AbstractChannel:
        """
        Returns the channel for sending messages within the session.

        The send channel is used to get or create the send queue.

        Parameters:
        None

        Returns:
        aio_pika.abc.AbstractChannel: The send channel for the session.

        Raises:
        SessionNotInitializedException: If the session is not initialized.
        """
        self._check_initialized()
        return self._send_channel
    
    @property
    def send_queue(self) -> aio_pika.abc.AbstractQueue:
        """
        Returns the queue for sendinng messages within the session.

        Parameters:
        None

        Returns:
        aio_pika.abc.AbstractQueue: The send queue for the session.

        Raises:
        SessionNotInitializedException: If the session is not initialized.
        """
        self._check_initialized()
        return self._send_queue
    
    @property
    def recieve_connection(self) -> aio_pika.abc.AbstractConnection:
        """
        Returns the connection for receiving messages within the session.

        The receive connection is used to establish a channel for receiving messages from the receive queue.

        Parameters:
        None

        Returns:
        aio_pika.abc.AbstractConnection: The receive connection for the session.

        Raises:
        SessionNotInitializedException: If the session is not initialized.
        """
        self._check_initialized()
        return self._recieve_connection
    
    @property
    def recieve_channel(self) -> aio_pika.abc.AbstractChannel:
        """
        Returns the channel for receiving messages within the session.

        The receive channel is used to get or create the receive queue.

        Parameters:
        None

        Returns:
        aio_pika.abc.AbstractChannel: The receive channel for the session.

        Raises:
        SessionNotInitializedException: If the session is not initialized.
        """
        self._check_initialized()
        return self._recieve_channel
    
    @property
    def recieve_queue(self) -> aio_pika.abc.AbstractQueue:
        """
        Returns the queue for receiving messages within the session.

        Parameters:
        None

        Returns:
        aio_pika.abc.AbstractQueue: The receive queue for the session.

        Raises:
        SessionNotInitializedException: If the session is not initialized.
        """
        self._check_initialized()
        return self._recieve_queue

    @property
    def id(self):
        """
        Returns the unique identifier of the session as a string.

        Parameters:
        None

        Returns:
        str: The unique identifier of the session.
        """
        return str(self.uuid)
    
    async def _main_callback(self, message: aio_pika.IncomingMessage, *args, **kwargs):
        """
        Asynchronously handles incoming messages by logging the message content, acknowledging it if auto_ack is True,
        and processing the message using the provided _process_message function if it's not None.

        Parameters:
        message (aio_pika.IncomingMessage): The incoming message to be handled.
        *args, **kwargs: Additional arguments and keyword arguments to be passed to the process_message function.

        Returns:
        None
        """
        logger.info(f"Recieved message: {message.body.decode()}")
        if self.auto_ack:
            await message.ack()
        if self._process_message is not None:
            res = self._process_message(msg=message, session=self, *args, **kwargs)
            if inspect.iscoroutine(res):
                await res

    def process_message(self, func: Union[Callable, Awaitable]):
        """
        Sets the function to be called when a new message is received.

        Parameters:
        func (Callable, Awaitable): The function to be called when a new message is received.
            This function should accept the following parameters:
            - msg (aio_pika.IncomingMessage): The incoming message.
            - session (BasicSession): The session object that received the message.
            - *args, **kwargs: Additional arguments and keyword arguments to be passed to the function.

        Returns:
        None
        """
        self._process_message = func

    async def send(self, message: str, **headers: str) -> str:
        """
        Asynchronously sends a message to the send queue.

        Parameters:
        message (str): The message to be sent.
        **headers (str): Additional headers to be included in the message.

        Returns:
        str: The message that was sent.

        Raises:
        Exception: If the session is not initialized.
        """
        self._check_initialized()
        await self.send_channel.default_exchange.publish(
            aio_pika.Message(message.encode(), headers=headers), 
            routing_key=f'send-{self.id}'
        )
        return message
    
    async def listen(self):
        """
        Asynchronously listens for incoming messages on the receive queue.

        This method sets up a consumer to listen for incoming messages on the receive queue.
        When a message is received, it is processed by the `_main_callback` method.
        The method logs the consumer details and waits for a KeyboardInterrupt to stop listening.

        Parameters:
        None

        Returns:
        None
        """
        self._listen_future = asyncio.get_event_loop().create_future()
        self._started_future = asyncio.get_event_loop().create_future()
        self._check_initialized()
        logger.info(f"Started listening queue recieve-{self.id}")
        consumer = await self.recieve_queue.consume(self._main_callback)
        logger.info(f"Listening consumer: {consumer}")
        logger.info("To exit press CTRL+C")
        self._started_future.set_result(True)
        try:
            await self._listen_future
        except KeyboardInterrupt:
            self._listen_future.set_result(True)
            logger.info(f"Interrupted consumer: {consumer}")

    async def stop(self):
        """
        Asynchronously closes the connections to the send and receive queues,
        and sets the result of the listen future to True to stop listening.

        **Note**: Can not be used directly after `await session.listen()`, but could be used, for example, in `_process_message` function

        Parameters:
        None

        Returns:
        None
        """
        await self.send_connection.close()
        await self.recieve_connection.close()
        self._listen_future.set_result(True)


class JSONSession(BasicSession):

    
    async def send(self, message: Dict, **headers: str) -> Dict:
        """
        Asynchronously sends a message to the send queue.
        
        This function converts the input message to JSON format before sending it to the send queue.
        It then awaits the super class's send method to actually send the message.
        Finally, it returns the message that was sent.

        Parameters:
        message (Dict): The message to be sent. This message will be converted to JSON format before sending.
        **headers (str): Additional headers to be included in the message.

        Returns:
        Dict: The message that was sent. This message will be the same as the input message, but in JSON format.

        """
        msg = json.dumps(message, ensure_ascii=False)
        await super().send(msg, **headers)
        return message
    
    def jsonify_body(self, func: Union[Callable, Awaitable]) -> Union[Callable, Awaitable]:
        """
        This method is a decorator that converts the incoming message body from JSON format.
        It is used to process incoming messages in a JSONSession or its subclasses.

        Parameters:
        func (Callable, Awaitable): The function to be decorated. This function should accept the following parameters:
            - msg (aio_pika.IncomingMessage): The incoming message.
            - *args, **kwargs: Additional arguments and keyword arguments to be passed to the function.

        Returns:
        Union[Callable, Awaitable]: The decorated function. This function will receive the incoming message body in JSON format,
        convert it to a Python dictionary, and pass it to the original function along with the other parameters.
        """

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
        """
        This method registers the function to be called when a new message is received.
        The function should accept the following parameters:
        - msg (aio_pika.IncomingMessage): The incoming message.
        - session (BasicSession): The session object that received the message.
        - *args, **kwargs: Additional arguments and keyword arguments to be passed to the function.

        The method converts the incoming message body from JSON format using the `jsonify_body` decorator.
        It then calls the super class's `process_message` method with the decorated function.

        Returns:
        Union[Callable, Awaitable]: The decorated function that will be called when a new message is received.
        """
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
        connection_parameters: ConnectionParameters, 
        uuid: Union[UUID, str] = None,
        auto_ack: bool = True,
        warn_on_reciever_differs: bool = True,
        *args, **kwargs
    ) -> None:
        """
        Creates a CommunicationSession object.

        Parameters:
        - communicator_name (str): The name of the communicator.
        - connection_parameters (ConnectionParameters): The connection parameters for the session.
        - uuid (Union[UUID, str], optional): The unique identifier for the session. Defaults to None.
        - auto_ack (bool, optional): Whether to automatically acknowledge received messages. Defaults to True.
        - warn_on_reciever_differs (bool, optional): Whether to warn if the reciever differs from the session's communicator name. Defaults to True.
        - *args, **kwargs: Additional arguments and keyword arguments to be passed to the superclass's constructor.

        Returns:
        - None
        """
        super().__init__(connection_parameters, uuid=uuid, auto_ack=auto_ack, *args, **kwargs)
        self.communicator_name = communicator_name
        self.warn_on_reciever_differs = warn_on_reciever_differs

    async def send(self, reciever: str, message: dict, answer_to: Optional[Union[UUID, str]] = None, message_id: Optional[Union[UUID, str]] = None, sender: str = None, **headres: str) -> MessageBodyDict:
        """
        Asynchronously sends a message to the send queue.
        
        This function constructs a message body with the provided parameters and sends it to the send queue.
        If the 'answer_to' parameter is provided, it is added to the message body.
        If the 'message_id' parameter is not provided, a new UUID will be generated and added to the message body.
        If the 'sender' parameter is not provided, the communicator's name will be used.
        The function then calls the superclass's 'send' method with the constructed message body and additional headers.
        Finally, it returns the constructed message body.

        Parameters:
        - reciever (str): The name of the receiver.
        - message (dict): The message to be sent.
        - answer_to (Optional[Union[UUID, str]]): The ID of the message to which this message is a response.
        - message_id (Optional[Union[UUID, str]]): The ID of the message. If not provided, a new UUID will be generated.
        - sender (str): The name of the sender. If not provided, the communicator's name will be used.
        - **headres (str): Additional headers for the message.

        Returns:
        - MessageBodyDict: A dictionary containing the message details.
        """
        
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
        return await super().send(body, **headres)
    
    def communicate_body(self, func: Union[Callable, Awaitable]) -> Union[Callable, Awaitable]:
        """
        This method is a decorator that processes the incoming message body in a CommunicationSession or its subclasses.
        It extracts the sender, receiver, message, message ID, and answer_to fields from the message body.
        It also checks if the sender is specified, if the receiver differs from the session's communicator name,
        and if the message and message ID are specified.
        If any of these conditions are not met, it logs a warning message.
        Finally, it calls the original function with the extracted fields and the session object as additional parameters.

        Parameters:
        - func (Callable, Awaitable): The function to be decorated. This function should accept the following parameters:
            - body (MessageBodyDict): The incoming message body.
            - *args, **kwargs: Additional parameters to be passed to the original function.

        Returns:
        - Union[Callable, Awaitable]: The decorated function. This function will receive the extracted fields and the session object,
        and pass them to the original function along with the other parameters.
        """
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
    
    def process_message(self, func: Callable) -> Callable:
        """
        This method registers the function to be called when a new message is received.
        Also this method decorates incoming function with communicate_body decorator.

        Parameters:
        - func (Callable): The function to be decorated. This function should accept the following parameters:
            - body (MessageBodyDict): The incoming message body.
            - *args, **kwargs: Additional parameters to be passed to the original function.

        Returns:
        - Callable: The decorated function. This function will receive the extracted fields and the session object,
        and pass them to the original function along with the other parameters.
        """
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
        """
        Creates a QueueSession object.

        Parameters:
        - communicator_name (str): The name of the communicator.
        - connection_parameters (ConnectionParameters): The connection parameters for the session.
        - uuid (Union[str, UUID], optional): The unique identifier for the session. Defaults to None.
        - auto_ack (bool, optional): Whether to automatically acknowledge received messages. Defaults to True.
        - warn_on_reciever_differs (bool, optional): Whether to warn if the reciever differs from the session's communicator name. Defaults to True.
        - *args, **kwargs: Additional arguments and keyword arguments to be passed to the superclass's constructor.

        Returns:
        - None
        """
        super().__init__(communicator_name, connection_parameters, uuid=uuid, auto_ack=auto_ack, warn_on_reciever_differs=warn_on_reciever_differs)
        self.awaiting_messages = {}
        self.callback_tasks = []
        self.process_message(lambda *args, **kwargs: None)

    async def send(self, reciever: str, message: dict, answer_to: Optional[Union[UUID, str]] = None, await_answer: bool = True, callback: Union[Callable, Awaitable] = None, message_id: Union[str, UUID] = None, sender: str = None, **headers: str) -> AwaitingMessageBodyDict:
        """
        Asynchronously sends a message to the send queue and awaits a response if await_answer is True.
        Also it creates an `asyncio.Future` for message. 
        The future will be completed when the QueueSession at some time later receives a message containing the `answer_to` field equal to the ID of the message sent now.
        The message with its future will be saved in `awaiting_messages` dict to check it in later `process_message` registered function call

        Parameters:
        - reciever (str): The name of the receiver.
        - message (dict): The message to be sent.
        - answer_to (Optional[Union[UUID, str]]): The ID of the message to which this message is a response. Defaults to None.
        - await_answer (bool): Whether to await a response. Defaults to True.
        - callback (Union[Callable, Awaitable]): The callback function to be called when a response is received. Defaults to None.
        - message_id (Union[str, UUID]): The ID of the message. If not provided, a new UUID will be generated. Defaults to None.
        - sender (str): The name of the sender. If not provided, the communicator's name will be used. Defaults to None.
        - **headers (str): Additional headers for the message.

        Returns:
        - AwaitingMessageBodyDict: A dictionary containing the message details, callback, future, and headers.
        """
        msg = await super().send(reciever, message, answer_to, message_id=message_id, sender=sender, **headers)
        if await_answer:
            msg['callback'] = callback
            msg['future'] = asyncio.get_event_loop().create_future()
            msg['_headers'] = headers
            self.awaiting_messages[str(msg['message_id'])] = msg
        return msg
    
    def awaitify_recieve(self, func: Union[Callable, Awaitable]) -> Union[Callable, Awaitable]:
        """
        A decorator function that performs a message future completion when QueueSession resieves a message with `answer_to` field.
        
        The decorated function will:
        1. Pop the message from the `awaiting_messages` dictionary using the `answer_to` parameter as the key.
        2. If the message is found, it will:
            - Set the result of the message to the future.
            - Call the callback function with the message, `answer_to`, and other keyword arguments.
            - If the callback function is a coroutine, it will be added to the `callback_tasks` list.
        3. Call the original function with the message, `answer_to`, and other keyword arguments.
        4. If the original function is a coroutine, it will be awaited.
         

        Parameters:
        - func (Callable, Awaitable): The function to be decorated.

        Returns:
        - Union[Callable, Awaitable]: The decorated function.
        """
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
    
    def process_message(self, func: Union[Callable, Awaitable]) -> None:
        """
        Registers a function to be called when a new message is received.
        The function will be decorated with the `awaitify_recieve` decorator,
        which will complete the future of a message when a response is received.

        Parameters:
        - func (Callable, Awaitable): The function to be registered.

        Returns:
        - None
        """
        new_func = self.awaitify_recieve(func)
        return super().process_message(new_func)
    
    async def listen(self) -> None:
        """
        Listens to the send queue and processes incoming messages.

        This function is an asynchronous method that listens to the send queue
        associated with the current session. It
        calls the superclass's listen method to consume a recieve queue and handle incoming messages. After
        stop listening, it awaits the completion of any pending
        callback tasks using asyncio.gather.

        Parameters:
        None

        Returns:
        None
        """
        result = await super().listen()
        await asyncio.gather(*self.callback_tasks)
        return result
        
        
class MSGAwaitSession(QueueSession):

    def __init__(self, communicator_name: str, connection_parameters: ConnectionParameters, uuid: str | UUID = None, auto_ack: bool = True, warn_on_reciever_differs: bool = True, *args, **kwargs) -> None:
        """
        Initialize a new instance of MSGAwaitSession.

        Parameters:
        - communicator_name (str): The name of the communicator.
        - connection_parameters (ConnectionParameters): The connection parameters for the session.
        - uuid (str | UUID): The unique identifier for the session. If not provided, a new UUID will be generated.
        - auto_ack (bool): Whether to automatically acknowledge received messages. Default is True.
        - warn_on_reciever_differs (bool): Whether to warn if the receiver differs from the session's communicator name. Default is True.
        - *args, **kwargs: Additional arguments to be passed to the superclass's constructor.

        Returns:
        - None
        """
        super().__init__(communicator_name, connection_parameters, uuid, auto_ack=auto_ack, warn_on_reciever_differs=warn_on_reciever_differs, *args, **kwargs)
        self.process_message(lambda *args, **kwargs: None)
        
    async def send_await(self, reciever: str, message: dict, answer_to: Optional[Union[UUID, str]] = None, await_answer: bool = True, callback: Callable[..., Any] = None, **headers: str) -> Any:
        """
        Asynchronously sends a message to the send queue and awaits a response if await_answer is True.
        The message is sent with the provided parameters and headers.
        If await_answer is True, the function waits for a response message with the same 'answer_to' field.
        The response message is added to the 'awaiting_messages' dictionary with its message ID as the key.
        The function returns the future of the response message.

        Parameters:
        - reciever (str): The name of the receiver.
        - message (dict): The message to be sent.
        - answer_to (Optional[Union[UUID, str]]): The ID of the message to which this message is a response. Defaults to None.
        - await_answer (bool): Whether to await a response. Defaults to True.
        - callback (Callable[..., Any]]): The callback function to be called when a response is received. Defaults to None.
        - **headers (str): Additional headers for the message.

        Returns:
        - Any: The result of the future of the response message. If await_answer is False, the function returns None.
        """
        message: AwaitingMessageBodyDict = await self.send(reciever, message, answer_to, await_answer, callback, **headers)
        self.awaiting_messages[str(message['message_id'])] = message
        future = message.get('future')
        if future is not None:
            await future
            return future.result()
        return None


class ReversedSession(BasicSession):

    async def send(self, message: str, **headers: str) -> str:
        """
        Asynchronously sends a message to the receive queue.
        Used in AT_REGISTRY brokers to handle components messages.

        Parameters:
        - message (str): The message to be sent.
        - **headers (str): Additional headers for the message.

        Returns:
        - str: The message that was sent.

        The function logs a warning message indicating that the reversed session is sending a message to the receive queue.
        It then calls the `recieve_queue_send` method to send the message and returns the sent message.
        """
        msg = f'Reversed Session {self.id} will send message {message} to recieve queue recieve-{self.id}'
        logger.warning(msg)
        return await self.recieve_queue_send(message, **headers)

    async def listen(self):
        """
        Asynchronously listens to the send queue and processes incoming messages.

        This function logs a warning message indicating that the reversed session is listening to the send queue.
        It then calls the `send_queue_listen` method to start listening and processing messages.
        The function returns the result of the `send_queue_listen` method.

        Parameters:
        None

        Returns:
        Any: The result of the `send_queue_listen` method.
        """
        msg = f'Reversed Session {self.id} will listen send queue send-{self.id}'
        logger.warning(msg)
        return await self.send_queue_listen()
    
    async def recieve_queue_send(self, message: str, **headers: str) -> str:
        """
        Asynchronously sends a message to the receive queue.
        Used in AT_REGISTRY brokers to send messages to their recieve queues.

        Parameters:
        - message (str): The message to be sent.
        - **headers (str): Additional headers for the message.

        Returns:
        - str: The message that was sent.

        The function logs a warning message indicating that the reversed session is sending a message to the receive queue.
        It then calls the `recieve_queue_send` method to send the message and returns the sent message.
        """
        self._check_initialized()
        await self.recieve_channel.default_exchange.publish(
            aio_pika.Message(message.encode(), headers=headers), 
            routing_key=f'recieve-{self.id}'
        )
        return message
    
    async def _send_callback(self, message: aio_pika.IncomingMessage, *args, **kwargs):
        """
        Asynchronously handles incoming messages from the send queue.

        Parameters:
        - message (aio_pika.IncomingMessage): The incoming message.
        - *args: Additional positional arguments.
        - **kwargs: Additional keyword arguments.

        Returns:
        - None

        This function logs the received message, acknowledges it if auto_ack is True,
        and processes it by calling the _process_message function if it is not None.
        If _process_message is a coroutine, it waits for its completion.
        """
        logger.info(f"Recieved message (in send queue): {message.body.decode()}")
        if self.auto_ack:
            await message.ack()
        if self._process_message is not None:
            res = self._process_message(msg=message, session=self, *args, **kwargs)
            if inspect.iscoroutine(res):
                await res

    async def send_queue_listen(self):
        """
        Asynchronously listens to the send queue and processes incoming messages.

        This function initializes a future for listening and checks if the session is initialized.
        It logs a message indicating that the reversed session is listening to the send queue.
        Then it consumes messages from the send queue using the `_send_callback` function.
        It logs messages indicating the listening consumer and instructions to exit.
        Finally, it sets the result of the `_started_future` to True and waits for the `_listen_future` to be completed.
        If a KeyboardInterrupt is raised, it sets the result of the `_listen_future` to True and logs an interrupted consumer message.

        Parameters:
        None

        Returns:
        None
        """
        self._listen_future = asyncio.get_event_loop().create_future()
        self._check_initialized()
        logger.info(f"Started reversed session listening queue send-{self.id}")
        consumer = await self.send_queue.consume(self._send_callback)
        logger.info(f"Listening consumer: {consumer}")
        logger.info("To exit press CTRL+C")
        self._started_future.set_result(True)
        try:
            await self._listen_future
        except KeyboardInterrupt:
            self._listen_future.set_result(True)
            logger.info(f"Interrupted consumer: {consumer}")


class JSONReversedSession(ReversedSession, JSONSession):
    
    async def send(self, message: dict, **headers: str) -> str:
        msg = json.dumps(message, ensure_ascii=False)
        await ReversedSession.send(self, msg, **headers)
        return message
    

class CommunicationReversedSession(JSONReversedSession, CommunicationSession):

    def __init__(self, communicator_name: str, connection_parameters: ConnectionParameters, uuid: UUID | str = None, auto_ack: bool = True, warn_on_reciever_differs: bool = True, *args, **kwargs) -> None:
        super().__init__(communicator_name, connection_parameters, uuid, auto_ack, *args, **kwargs)
        self.warn_on_reciever_differs = warn_on_reciever_differs

    async def send(self, reciever: str, message: dict, answer_to: Optional[Union[UUID, str]] = None, message_id: Optional[Union[UUID, str]] = None, sender: str = None, warn_on_reciever_differs: bool = True, **headers: str) -> MessageBodyDict:
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
        return await JSONReversedSession.send(self, body, **headers)