import sys
sys.path.append('gen-py')

import contextlib

from dvidmsg import BenchmarkService

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import numpy
import conversions

try:
    transport = TSocket.TSocket('localhost', 9090)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    
    client = BenchmarkService.Client(protocol)
    transport.open()

    with contextlib.closing( transport ) as transport:
        send_data = numpy.random.random( (10,100,100) )
        send_data *= 100
        send_data = send_data.astype( numpy.int32 )
        send_msg = conversions.convert_array_to_dvidmsg( send_data )
        rcv_msg = client.echoData(send_msg)
        rcv_data = conversions.convert_array_from_dvidmsg( rcv_msg )

        assert rcv_msg.description == send_msg.description, "Message description was not echoed correctly."
        assert rcv_msg.data == send_msg.data, "Message data was not echoed correctly."
        assert rcv_msg == send_msg, "Message was not echoed correctly."
        assert (rcv_data == send_data).all(), "Data mismatch after conversion"
        
        print "Received correct echo!"

except Thrift.TException, tx:
    print '%s' % (tx.message)
