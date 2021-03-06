import collections
import numpy

import genpy.dvidmsg
from genpy.dvidmsg.ttypes import Array, DatasetDescription, Bounds, ArrayData

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
                     conversion_spec('float64', numpy.float64, numpy.float64, 'dataDouble') ]

conversion_specs_from_dvid = { spec.dvid_type : spec for spec in conversion_specs }
conversion_specs_from_numpy = { spec.numpy_type : spec for spec in conversion_specs }

def convert_array_from_dvidmsg(msg):
    _, thrift_dtype, dtype, msg_field = conversion_specs_from_dvid[msg.description.datatype]
    msg_data = getattr( msg.data, msg_field )
    
    shape = numpy.array(msg.description.bounds.stop) - msg.description.bounds.start
    assert numpy.prod(shape) == len(msg_data), \
        "Array from server has mismatched length and shape: {}, {}".format( shape, len(msg_data) )

    # Use Fortran order, since that's the DVID convention
    result = numpy.ndarray( shape, dtype=dtype, order='F' )

    # Use ndarray.view() to reinterpret the type of our 
    #  destination so no conversions are performed when copying the raw bytes over.
    view = result.view(thrift_dtype)

    # numpy arrays are always indexed in C-order, 
    #  so we have to transpose before assigning the flat data.
    view.transpose().flat[:] = msg_data

    return result

def convert_array_to_dvidmsg(a, dvid_start=None):
    msg = Array()
    msg.description = DatasetDescription(bounds=Bounds(), axisNames=[])
    msg.data = ArrayData()
    if dvid_start is None:
        dvid_start = (0,) * len(a.shape)
    msg.description.bounds.start = list(dvid_start)
    msg.description.bounds.stop = list(numpy.array(dvid_start) + a.shape)

    dvid_type, thrift_dtype, _, msg_field = conversion_specs_from_numpy[a.dtype.type]
    msg.description.datatype = dvid_type

    # We assume this array is provided in fortran order, because that's the dvid convention.
    # But we need to transmit in C-order, so transpose() to get correct iteration order.
    flat_view = a.transpose().flat[:]
    
    # No need to copy into a list here.  Just provide the view itself.
    setattr( msg.data, msg_field, flat_view.view( thrift_dtype ) )

    return msg

