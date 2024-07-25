from at_queue.core.session import ConnectionParameters
from at_queue.core.at_registry import ATRegistry
import argparse
import asyncio
import logging
import os

parser = argparse.ArgumentParser(
    prog='at-queue',
    description='AT-TECHNOLOGY message queue component')

parser.add_argument('-u', '--url', help="RabbitMQ URL to connect", required=False, default=None)
parser.add_argument('-H', '--host', help="RabbitMQ host to connect", required=False, default="localhost")
parser.add_argument('-p', '--port', help="RabbitMQ port to connect", required=False, default=5672)
parser.add_argument('-L', '--login', '-U', '--user', '--user-name', '--username', '--user_name', dest="login", help="RabbitMQ login to connect", required=False, default="guest")
parser.add_argument('-P', '--password', help="RabbitMQ password to connect", required=False, default="guest")
parser.add_argument('-v',  '--virtualhost', '--virtual-host', '--virtual_host', dest="virtualhost", help="RabbitMQ virtual host to connect", required=False, default="/")


async def main(**connection_kwargs):
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
        
    await registry.start()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    args_dict = vars(args)

    asyncio.run(main(**args_dict))