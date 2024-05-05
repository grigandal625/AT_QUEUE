from at_queue.core.at_broker import ATBrokerInstance
from at_queue.core.at_component import ATComponent
from at_queue.core.session import ConnectionParameters, CommunicationReversedSession
from at_queue.core.at_component import BaseComponent, BaseComponentMethod, Input, Output
from typing import Dict, Union
from uuid import UUID, uuid3, NAMESPACE_OID
import asyncio
import logging


logger = logging.getLogger(__name__)


class ATRegistry:
    _registry: Dict[str, ATBrokerInstance]
    session: CommunicationReversedSession
    initialized: bool = False
    _brokers_listen_tasks: Dict[str, asyncio.Task]

    def __init__(self, connection_parameters: ConnectionParameters, session_id: Union[str, UUID] = None) -> None:
        session_id = session_id or uuid3(NAMESPACE_OID, 'at_registry')
        self._registry = {}
        self._brokers_listen_tasks = {}
        self.session = CommunicationReversedSession('registry', connection_parameters, session_id)

    async def initialize(self):
        await self.session.initialize()
        self.session.process_message(self._on_register)
        self.initialized = True

    async def _on_register(self, *args, message: dict, sender: str, reciever: str, message_id: str, **kwargs):
        if message.get('type') not in ['register', 'inspect']:
            logger.warning(f'Recieved register message {message} with id {message_id} from {sender}')
        if reciever != 'registry':
            logger.warning(f'Recieved register message {message} with id {message_id} from {sender} that is not sent to "registry" but to "{reciever}"')
        
        if message.get('type') == 'inspect':
            if sender != 'inspector':
                logger.warning(f'Recieved register message {message} with id {message_id} from {sender}')
            component_name = message.get('component')
            if component_name is None:
                return await self.session.send(sender, {'errors': ["Field \"component\" (str) is required"]}, answer_to=message_id)
            broker = self._registry.get(component_name)
            if broker is None:
                return await self.session.send(sender, {'errors': [f'Component "{component_name}" is not registered']}, answer_to=message_id)
            return await self.session.send(sender, {
                'broker': {
                    'session_id': str(broker.session.uuid)
                },
                'component': broker.component.__dict__
            }, answer_to=message_id)
        
        component_data = message.get('component')
        component = self._build_component(component_data)
        session_id = message.get('session_id') or uuid3(NAMESPACE_OID, component.name)
        broker = ATBrokerInstance(self, component, self.session.connection_parameters, session_id)
        await broker.initialize()
        answer = {
            'session_id': str(session_id),
        }
        self._registry[component.name] = broker
        self._brokers_listen_tasks[component.name] = asyncio.get_event_loop().create_task(broker.start())
        await self.session.send(component.name, answer, answer_to=message_id)

    def _build_component(self, component_data: dict) -> BaseComponent:
        name = component_data.get('name')
        description = component_data.get('description')
        methods_data = component_data.get('methods')
        methods = {
            method_name: self._build_method(method_data)
            for method_name, method_data in methods_data.items()
        }
        return BaseComponent(name=name, description=description, methods=methods)
    
    def _build_method(self, method_data: dict):
        name = method_data.get('name')
        description = method_data.get('description')
        inputs_data = method_data.get('inputs')
        output_data = method_data.get('output')
        inputs = {
            input_name: self._build_input(input_data)
            for input_name, input_data in inputs_data.items()
        }
        output = self._build_output(output_data)
        return BaseComponentMethod(name=name, description=description, inputs=inputs, output=output)

    def _build_input(self, input_data: dict):
        return Input(**input_data)
    
    def _build_output(self, output_data: dict):
        return Output(**output_data)

    def get_broker(self, name: str):
        return self._registry.get(name)
    
    async def start(self):
        return await self.session.listen()
    
    async def stop(self):
        for name in self._registry:
            broker = self._registry[name]
            await broker.stop()
            start_task = self._brokers_listen_tasks.get(name)
            if start_task:
                await start_task
        await self.session.stop()


class ATRegistryInspector(ATComponent):

    def __init__(self, connection_parameters: ConnectionParameters, *args, **kwargs):
        kwargs['name'] = 'inspector'
        super().__init__(connection_parameters, *args, **kwargs)

    async def inspect(self, component):
        return await self.session.send_await(
            'registry',
            {
                'type': 'inspect',
                'component': component
            }
        )