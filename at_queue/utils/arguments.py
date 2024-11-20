import argparse

parser = argparse.ArgumentParser(
    prog='at-queue',
    description='AT-TECHNOLOGY message queue component')

parser.add_argument('-u', '--url', help="RabbitMQ URL to connect", required=False, default=None)
parser.add_argument('-H', '--host', help="RabbitMQ host to connect", required=False, default="localhost")
parser.add_argument('-p', '--port', help="RabbitMQ port to connect", required=False, default=5672, type=int)
parser.add_argument('-L', '--login', '-U', '--user', '--user-name', '--username', '--user_name', dest="login", help="RabbitMQ login to connect", required=False, default="guest")
parser.add_argument('-P', '--password', help="RabbitMQ password to connect", required=False, default="guest")
parser.add_argument('-v',  '--virtualhost', '--virtual-host', '--virtual_host', dest="virtualhost", help="RabbitMQ virtual host to connect", required=False, default="/")

parser.add_argument('-dh', '--debugger-host', '--debugger_host', help="Debugger server host", required=False, default="127.0.0.1")
parser.add_argument('-dp', '--debugger-port', '--debugger_port', help="Debugger server port", required=False, default=8080, type=int)

def get_args():
    args = parser.parse_args()
    res = vars(args)
    res.pop('debugger', False)
    return res