from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from at_queue.core.at_registry import ATRegistryInspector, ConnectionParameters
from at_queue.utils.arguments import get_args
from at_queue.debug.models import ExecMetod, ExecMethodResult
from at_queue.core.exceptions import ATQueueException
from typing import Dict
from uvicorn import Config, Server
import asyncio

class GLOBAL:
    inspector: ATRegistryInspector = None

async def get_inspector():
    inspector = GLOBAL.inspector
    if inspector is None:
        args = get_args()
        args.pop('debugger_host', None)
        args.pop('debugger_port', None)
        connection_parameters = ConnectionParameters(**args)
        inspector = ATRegistryInspector(connection_parameters)
    if not inspector.initialized:
        await inspector.initialize()
    if not inspector.registered:
        await inspector.register()
    GLOBAL.inspector = inspector
    return inspector

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/api/components')
async def get_components() -> Dict:
    inspector = await get_inspector()
    components = await inspector.inspect_all()
    return components


@app.post('/api/exec_method')
async def exec_method(data: ExecMetod) -> ExecMethodResult:
    inspector = await get_inspector()
    try:
        result = await inspector.exec_external_method(data.component, data.method, data.kwargs, auth_token=data.auth_token)
    except ATQueueException as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=e.__dict__)
    return {'result': result}


async def main():
    inspector = await get_inspector()
    loop = asyncio.get_event_loop()
    task = None
    if not inspector.started:
        task = loop.create_task(inspector.start())
    args = get_args()
    config = Config(app=app, loop=loop, host=args.get('debugger_host', '127.0.0.1'), port=args.get('debugger_port', 8080))
    server = Server(config)
    await server.serve()
    if task is not None:
        await task