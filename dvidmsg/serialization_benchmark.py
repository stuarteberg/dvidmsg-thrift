import sys
sys.path.append('gen-py')

import contextlib
import numpy
from lazyflow.utility import Timer

from thrift import Thrift
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from dvidmsg.ttypes import Array
import conversions

try:
    # Prepare array data
    send_data = numpy.random.random( (100,100,100) )
    send_data *= 100
    send_data = send_data.astype( numpy.int32 )
    data_mb = numpy.prod( send_data.shape ) * send_data.dtype.type().nbytes / 1e6
    print "Array data is {} MB".format( data_mb )

    # Format into message
    with Timer() as timer:
        send_msg = conversions.convert_array_to_dvidmsg( send_data )
    print "Formatting message structure took {} seconds".format( timer.seconds() )

    # Send it
    send_transport = TTransport.TMemoryBuffer()
    protocol = TBinaryProtocol.TBinaryProtocol(send_transport)    
    with contextlib.closing( send_transport ):
        with Timer() as timer:
            send_msg.write(protocol)
        print "Serialization took {} seconds".format( timer.seconds() )
        
        # Copy the buffer data before we close the transport.
        buf = send_transport.cstringio_buf
        buf.seek(0)
        buffer_data = buf.read()
        print "Serialized data is {} MB".format( len(buffer_data) / 1e6 )

    # Read it
    rcv_transport = TTransport.TMemoryBuffer(buffer_data)
    protocol = TBinaryProtocol.TBinaryProtocol(rcv_transport)
    with contextlib.closing( rcv_transport ):
        rcv_msg = Array()
        with Timer() as timer:
            rcv_msg.read( protocol )
        print "Deserialization took {} seconds".format( timer.seconds() )

    # Convert back to ndarray
    with Timer() as timer:
        rcv_data = conversions.convert_array_from_dvidmsg( rcv_msg )
    print "Converting message structure to ndarray took {} seconds".format( timer.seconds() )

    # Check the results.
    print "Checking for errors..."
    assert rcv_msg.description == send_msg.description, "Message description was not echoed correctly."
    assert (rcv_data == send_data).all(), "Data mismatch after conversion"
    
    print "Successful round-trip (de)serialization!"

except Thrift.TException, tx:
    print '%s' % (tx.message)
