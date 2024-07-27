from at_queue.core.session import ConnectionParameters
from at_queue.core.at_registry import ATRegistry
import argparse
import asyncio
import logging
import os
from at_queue.utils.arguments import parser
from at_queue.debug.server import main as debugger_main


async def main(**connection_kwargs):
    loop = asyncio.get_event_loop()
    connection_parameters = ConnectionParameters(**connection_kwargs)
    registry = ATRegistry(connection_parameters)

    await registry.initialize()
    try:
        if not os.path.exists('/var/run/at_queue/'):
            os.makedirs('/var/run/at_queue/')

        with open('/var/run/at_queue/pidfile.pid', 'w') as f:
            f.write(str(os.getpid()))
    except PermissionError:
        pass
        
    task = loop.create_task(registry.start())
    await registry.session._started_future
    await debugger_main()
    await task


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    args_dict = vars(args)

    asyncio.run(main(**args_dict))