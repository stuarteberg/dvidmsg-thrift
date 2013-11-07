import sys
sys.path.append('gen-py')

import contextlib

import h5py

from dvidmsg import CutoutService
from dvidmsg.ttypes import Bounds, DatasetDescription

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

import conversions

def make_slicing(start, stop):
    return tuple(slice(a, b) for a,b in zip(start, stop))

class CutoutServiceHandler(object):

    def __init__(self, hdf5_filename):
        self._f = h5py.File( hdf5_filename, 'r' )

    def close(self):
        self._f.close()

    def listDatasets(self):
        l = []
        def append_if_dataset(key, value):
            if isinstance(value, h5py.Dataset):
                l.append(key)
        self._f.visititems(append_if_dataset)
        return l

    def getDescription(self, datasetName):
        description = DatasetDescription()
        dataset = self._f[datasetName]
        n_dims = len( dataset.shape )
        
        description.bounds = Bounds()
        description.bounds.start = [0] * n_dims
        description.bounds.stop = dataset.shape
        
        # FIXME: Using default axis labels for now.
        description.axisNames = 'tzyxc'[-n_dims:]
        description.datatype = conversions.conversion_specs_from_numpy[dataset.dtype.type].dvid_type
        
        return description
    
    def getCutoutBlock(self, datasetName, subregion):
        dataset = self._f[datasetName]
        slicing = make_slicing(subregion.start, subregion.stop)
        data = dataset[slicing]
        msg = conversions.convert_array_to_dvidmsg(data, subregion.start)

        # FIXME: Using default axis labels for now.
        n_dims = len( dataset.shape )
        msg.axisNames = 'tzyxc'[-n_dims:]
        return msg

filename = sys.argv[1]

with contextlib.closing( CutoutServiceHandler(filename) ) as handler:
    processor = CutoutService.Processor(handler)
    transport = TSocket.TServerSocket(port=9090)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    
    server.serve()
