import sys
sys.path.append('gen-py')

from dvidmsg import BenchmarkService

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

class BenchmarkServiceHandler(object):
    def echoData(self, array):
        return array

handler = BenchmarkServiceHandler()
processor = BenchmarkService.Processor(handler)
transport = TSocket.TServerSocket(port=9090)
tfactory = TTransport.TBufferedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolFactory()

server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

server.serve()
