import contextlib
import numpy
from timer import Timer

from thrift import Thrift
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from genpy.dvidmsg.ttypes import Array
import thrift_conversions

class BenchmarkStats(object):
    def __init__(self):
        self.type_name = None
        self.image_size_mb = None
        self.message_size_mb = None
        self.message_creation_seconds = None
        self.serialization_seconds = None
        self.deserialization_seconds = None
        self.array_creation_seconds = None

def random_data( dtype, shape ):
    if numpy.issubdtype(dtype, numpy.integer):
        send_data = numpy.random.randint( numpy.iinfo(dtype).min/2, numpy.iinfo(dtype).max/2, shape )
        send_data = send_data.astype(dtype)
    else:
        send_data = numpy.random.random( shape )
        send_data = send_data.astype( dtype )
    return send_data

def run_benchmark( dtype, shape ):
    try:
        stats = BenchmarkStats()
        stats.type_name = thrift_conversions.conversion_specs_from_numpy[ dtype ].dvid_type
    
        # Prepare array data
        send_data = random_data( dtype, shape )
        stats.image_size_mb = numpy.prod( send_data.shape ) * send_data.dtype.type().nbytes / 1e6
        
        # Format into message
        with Timer() as timer:
            send_msg = thrift_conversions.convert_array_to_dvidmsg( send_data )
        stats.message_creation_seconds = timer.seconds()
    
        # Serialize it
        send_transport = TTransport.TMemoryBuffer()
        protocol = TBinaryProtocol.TBinaryProtocol(send_transport)    
        with contextlib.closing( send_transport ):
            with Timer() as timer:
                send_msg.write(protocol)
            stats.serialization_seconds = timer.seconds()
            #print "Serialization took {} seconds".format( timer.seconds() )
            
            # Copy the buffer data before we close the transport.
            buf = send_transport.cstringio_buf
            buf.seek(0)
            buffer_data = buf.read()
            stats.message_size_mb = len(buffer_data) / 1e6
            #print "Serialized data is {} MB".format( len(buffer_data) / 1e6 )
    
        # Deserialize
        rcv_transport = TTransport.TMemoryBuffer(buffer_data)
        protocol = TBinaryProtocol.TBinaryProtocol(rcv_transport)
        with contextlib.closing( rcv_transport ):
            rcv_msg = Array()
            with Timer() as timer:
                rcv_msg.read( protocol )
            stats.deserialization_seconds = timer.seconds()
            #print "Deserialization took {} seconds".format( timer.seconds() )
    
        # Convert back to ndarray
        with Timer() as timer:
            rcv_data = thrift_conversions.convert_array_from_dvidmsg( rcv_msg )
        stats.array_creation_seconds = timer.seconds()
        #print "Converting message structure to ndarray took {} seconds".format( timer.seconds() )
    
        # Check the results.
        #print "Checking for errors..."
        assert rcv_msg.description == send_msg.description, "Message description was not echoed correctly."
        assert (rcv_data == send_data).all(), "Data mismatch after conversion (dtype={}, shape={})".format( dtype, shape )
        
        #print "Successful round-trip (de)serialization!"
    
    except Thrift.TException, tx:
        print '%s' % (tx.message)
    
    return stats

def print_stats_header():
    header = "image size,"\
             "dtype,"\
             "encoded message size,"\
             "message creation time,"\
             "serialization time,"\
             "deserialization time,"\
             "array creation time"
    print header

def print_stats( stats ):
    stats_row = ( "{image_size_mb},"
                  "{type_name},"
                  "{message_size_mb},"
                  "{message_creation_seconds},"
                  "{serialization_seconds},"
                  "{deserialization_seconds},"
                  "{array_creation_seconds}"
                  "".format( **stats.__dict__ ) )
    print stats_row

def main():
    shapes = [ (100,100),
               (100,1000),
               (1000,1000),
               (10,1000,1000),
               (100,1000,1000),
               (1000,1000,1000) ]
    
    print_stats_header()
    
    dtypes = [ numpy.uint8, numpy.int8,
               numpy.uint16, numpy.int16,
               numpy.uint32, numpy.int32,
               numpy.float32, 
               numpy.uint64, numpy.int64,
               numpy.float64 ]
    for shape in shapes:
        for dtype in dtypes:
            print_stats( run_benchmark( dtype, shape ) )
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit( main() )
