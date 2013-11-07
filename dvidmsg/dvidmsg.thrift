/**
 * The available types in Thrift are:
 *
 *  bool        Boolean, one byte
 *  byte        Signed byte
 *  i16         Signed 16-bit integer
 *  i32         Signed 32-bit integer
 *  i64         Signed 64-bit integer
 *  double      64-bit floating point value
 *  string      String
 *  binary      Blob (byte array)
 *  map<t1,t2>  Map from one type to another
 *  list<t1>    Ordered list of one type
 *  set<t1>     Set of unique elements of one type
 */

namespace * dvidmsg

typedef list<i32> Coord
typedef string AxisName
typedef list<AxisName> AxisNames # Each must be one of 't','x','y','z', or 'c' (no duplicates)
typedef string dtype # Must be one of 'i8', 'u8', 'i16', 'u16', 'i32', 'u32', 'float32', 'float64'

struct Bounds {
    1: Coord start,
    2: Coord stop
}

struct DatasetDescription {
    1: Bounds bounds,
    2: AxisNames axisNames,
    3: dtype datatype
}

struct Array {
    1: Bounds subregion,
    2: AxisNames axisNames,
    3: dtype datatype,
    
    # Only one of the following fields will have any data,
    #  which determines the dtype of the result.
    4: optional list<byte> data8, # Used for i8,u8
    5: optional list<i16> data16, # Used for i16, u16
    6: optional list<i32> data32, # Used for i32, u32, AND float32
    7: optional list<i64> data64, # Used for i64, u64
    8: optional list<double> dataDouble # Used for float64
}

service CutoutService {
    list<string> listDatasets()
    DatasetDescription getDescription(1: string datasetName)    
    Array getCutoutBlock(1: string datasetName, 2: Bounds subregion) # TODO: Add param for dag node uuid
}

