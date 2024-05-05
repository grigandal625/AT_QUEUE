from at_queue.core.session import CommunicationReversedSession
from at_queue.core.at_component import BaseComponent
from at_queue.core.session import ConnectionParameters
from typing import Union, TYPE_CHECKING
from uuid import UUID
from aio_pika import IncomingMessage

if TYPE_CHECKING:
    from at_queue.core.at_registry import ATRegistry


import logging

logger = logging.getLogger(__name__)



class ATBrokerInstance:
    component: BaseComponent
    session: CommunicationReversedSession
    connection_parameters: ConnectionParameters
    initialized: bool = False
    registry: 'ATRegistry'

    def __init__(self, registry: 'ATRegistry', component: BaseComponent, connection_parameters: ConnectionParameters, session_id: Union[str, UUID]) -> None:
        self.registry = registry
        self.component = component
        self.connection_parameters = connection_parameters
        self.session = CommunicationReversedSession(
            self.component.name, 
            self.connection_parameters,
            uuid=session_id
        )

    async def initialize(self) -> bool:
        await self.session.initialize()
        self.session.process_message(self._log_process_message)
        self.initialized = True

    async def _log_process_message(self, *args, message: dict, sender: str, reciever: str, message_id: str, **kwargs):
        logger.info(f'Start for "{self.component.name}" broker processing sent message {message_id}: {message} from "{sender}" with arguments: {args}, {kwargs}')
        if sender != self.component.name:
            logger.warning(f'Component "{self.component.name}" sended message that is not from "{self.component.name}", but from "{reciever}"')

        await self._process_message(message=message, sender=sender, reciever=reciever, message_id=message_id, *args, **kwargs)

        logger.info(f'Finish for "{self.component.name}" broker processing message {message_id}')

    async def _process_message(self, *, message: dict, sender: str, reciever: str, message_id: Union[str, UUID], answer_to: Union[str, UUID], msg: IncomingMessage, **kwargs):
        if reciever == 'registry':
            if message.get('type') == 'check_registered':
                component = message.get('component')
                await self.session.send(sender, {'result': component in self.registry}, answer_to=message_id, sender='registry')
            elif message.get('type') == 'inspect':
                if sender != 'inspector':
                    logger.warning(f'Recieved register message {message} with id {message_id} from {sender}')
                component_name = message.get('component')
                if component_name is None:
                    return await self.session.send(sender, {'errors': ["Field \"component\" (str) is required"]}, answer_to=message_id)
                broker = self.registry._registry.get(component_name)
                if broker is None:
                    return await self.session.send(sender, {'errors': [f'Component "{component_name}" is not registered']}, answer_to=message_id)
                return await self.session.send(sender, {
                    'broker': {
                        'session_id': str(broker.session.uuid)
                    },
                    'component': broker.component.__dict__
                }, answer_to=message_id)
        else:
            reciever_broker = self.registry.get_broker(reciever)
            if reciever_broker:
                await reciever_broker.session.send(reciever, message, answer_to=answer_to, message_id=message_id, sender=sender, **msg.headers)
            else:
                await self.session.send(sender, {'errors': [f'Component "{reciever}" is not registered yet']}, answer_to=message_id, sender='registry')

    async def start(self):
        return await self.session.listen()
    
    async def stop(self):
        await self.session.stop()