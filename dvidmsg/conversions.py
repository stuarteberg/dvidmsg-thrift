import sys
import collections
import numpy

sys.path.append('gen-py')
from dvidmsg.ttypes import Array, Bounds

conversion_spec = collections.namedtuple('conversion_spec', 'dvid_type thrift_type numpy_type msg_field')

conversion_specs = [ conversion_spec('i8',      numpy.int8,  numpy.int8,    'data8'),
                     conversion_spec('u8',      numpy.int8,  numpy.uint8,   'data8'),
                     conversion_spec('i16',     numpy.int16, numpy.int16,   'data16'),
                     conversion_spec('u16',     numpy.int16, numpy.uint16,  'data16'),
                     conversion_spec('i32',     numpy.int32, numpy.int32,   'data32'),
                     conversion_spec('u32',     numpy.int32, numpy.uint32,  'data32'),
                     conversion_spec('i64',     numpy.int64, numpy.int64,   'data64'),
                     conversion_spec('u64',     numpy.int64, numpy.uint64,  'data64'),
                     conversion_spec('float32', numpy.int32, numpy.float32, 'data32'),
                     conversion_spec('float64', numpy.int64, numpy.float64, 'dataDouble') ]

conversion_specs_from_dvid = { spec.dvid_type : spec for spec in conversion_specs }
conversion_specs_from_numpy = { spec.numpy_type : spec for spec in conversion_specs }

def convert_array_from_dvidmsg(msg):
    _, thrift_dtype, dtype, msg_field = conversion_specs_from_dvid[msg.datatype]
    msg_data = getattr(msg, msg_field)
    
    shape = numpy.array(msg.subregion.stop) - msg.subregion.start
    assert numpy.prod(shape) == len(msg_data), \
        "Array from server has mismatched length and shape: {}, {}".format( shape, len(msg_data) )

    # Use Fortran order under the hood, since that's the order that DVID gives us.
    result = numpy.ndarray( shape, dtype=dtype, order='F' )

    # Use ndarray.view() to reinterpret the type of our 
    #  destination so no conversions are performed when copying the raw bytes over.
    view = result.view(thrift_dtype)

    # numpy arrays are always indexed in C-order, 
    #  so we have to transpose before assigning the flat data.
    view.transpose().flat[:] = msg_data

    return result

def convert_array_to_dvidmsg(a, dvid_start):
    msg = Array()
    msg.subregion = Bounds()
    msg.subregion.start = dvid_start
    msg.subregion.stop = numpy.array(dvid_start) + a.shape
    msg.data8 = []
    msg.data16 = []
    msg.data32 = []
    msg.data64 = []
    msg.dataDouble = []

    dvid_type, thrift_dtype, _, msg_field = conversion_specs_from_numpy[a.dtype.type]
    msg_data = getattr(msg, msg_field)
    msg.datatype = dvid_type

    # DVID expects fortran order, so transpose before flattening.
    flat_view = a.transpose().flat[:]
    msg_data[:] = flat_view.view( thrift_dtype )

    return msg

