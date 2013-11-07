import sys
sys.path.append('gen-py')

import contextlib

from dvidmsg import CutoutService
from dvidmsg.ttypes import Bounds

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
    
    client = CutoutService.Client(protocol)
    transport.open()

    with contextlib.closing( transport ) as transport:
        names = client.listDatasets()
        print "Found datasets: "
        for d in names:
            print d
        print ""

        for name in names:        
            description = client.getDescription(name)
            print "Info for {}:".format(name)
            print "bounds: {}, {}".format( description.bounds.start, description.bounds.stop )
            print "axes:", description.axisNames
            print "dtype:", description.datatype
            print ""

            # Print some of it
            start, stop = numpy.array(description.bounds.start), numpy.array(description.bounds.stop)
            array_msg = client.getCutoutBlock(name, Bounds(start=start + stop/2, stop=stop))
            data = conversions.convert_array_from_dvidmsg( array_msg )
            print 'numpy array has dtype:', data.dtype
            print data

except Thrift.TException, tx:
    print '%s' % (tx.message)
