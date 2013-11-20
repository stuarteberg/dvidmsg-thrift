// serialization_benchmark.cpp
#include <iostream>
#include <iomanip>
#include <vector>

#include <boost/cstdint.hpp>
#include <boost/assign/list_of.hpp>
#include <boost/chrono.hpp>
#include <boost/foreach.hpp>
#include <boost/random.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/static_assert.hpp>
#include <boost/numeric/conversion/bounds.hpp>
#include <boost/limits.hpp>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include "gen-cpp/BenchmarkService.h"
#include "timer.h"

using namespace ::apache::thrift ;
using namespace ::apache::thrift::protocol ;
using namespace ::apache::thrift::transport ;
using namespace ::apache::thrift::server ;

using boost::int8_t;
using boost::uint8_t;
using boost::int16_t;
using boost::uint16_t;
using boost::int32_t;
using boost::uint32_t;
using boost::int64_t;
using boost::uint64_t;

using namespace ::dvidmsg;

typedef boost::shared_ptr<Array> ArrayPtr ;

// ******************************************************************
// @details
// Return random number uniformly distributed within the range of
//  the specified numeric type.
// ******************************************************************
template <typename T_>
T_ get_uniform_random()
{
    static const T_ lowest = boost::numeric::bounds<T_>::lowest() ;
    static const T_ highest = boost::numeric::bounds<T_>::highest() ;

    static boost::random::uniform_int_distribution<T_> dist(lowest, highest);
    static boost::random::mt19937 gen;
    return dist(gen) ;
}

template <>
float get_uniform_random<float>()
{
    // For some reason using the true max/min possible values is very slow.
    //static const double lowest = boost::numeric::bounds<float>::lowest() ;
    //static const double highest = boost::numeric::bounds<float>::highest() ;
    //static boost::random::uniform_real_distribution<float> dist(lowest, highest);

    // Just use max/min of -1/1 for speed.  Should be almost random data (except exponent...)
    static boost::random::uniform_real_distribution<float> dist(-1.0, 1.0);
    static boost::random::mt19937 gen;
    return dist(gen) ;
}

template <>
double get_uniform_random<double>()
{
    // For some reason using the true max/min possible values is very slow.
    //static const double lowest = boost::numeric::bounds<double>::lowest() ;
    //static const double highest = boost::numeric::bounds<double>::highest() ;
    //static boost::random::uniform_real_distribution<double> dist(lowest, highest);

    // Just use max/min of -1/1 for speed
    static boost::random::uniform_real_distribution<float> dist(-1.0, 1.0);
    static boost::random::mt19937 gen;
    return dist(gen) ;
}

// ******************************************************************
// @details
// Populate a vector with uniformly distributed random data
// ******************************************************************
template <typename T_>
void fill_random( std::vector<T_> & vec, size_t len )
{
    vec.resize(len);
    BOOST_FOREACH( T_ & x, vec )
    {
        x = get_uniform_random<T_>() ;
    }
}

// ******************************************************************
// @details
// This function is specialized for each numeric type to return the
//  type name string used in the DVID messaging spec.
// ******************************************************************
template <typename T_> std::string get_type_name()
{ BOOST_STATIC_ASSERT( sizeof(T_) == 0 && "Type name string not defined for this type!" ) ; }
template <> std::string get_type_name<uint8_t>()    { return "u8" ; }
template <> std::string get_type_name<int8_t>()     { return "i8" ; }
template <> std::string get_type_name<uint16_t>()   { return "u16" ; }
template <> std::string get_type_name<int16_t>()    { return "i16" ; }
template <> std::string get_type_name<uint32_t>()   { return "u32" ; }
template <> std::string get_type_name<int32_t>()    { return "i32" ; }
template <> std::string get_type_name<uint64_t>()   { return "u64" ; }
template <> std::string get_type_name<int64_t>()    { return "i64" ; }
template <> std::string get_type_name<float>()      { return "float32" ; }
template <> std::string get_type_name<double>()     { return "float64" ; }

template <typename T_>
void populate_description( DatasetDescription & description )
{
    description.bounds.start = boost::assign::list_of(0)(0)(0)(0)(0);
    description.bounds.stop = boost::assign::list_of(0)(0)(0)(0)(0);
    description.axisNames = boost::assign::list_of("t")("x")("y")("z")("c") ;
    description.datatype = get_type_name<T_>() ;
}

template <typename T_>
void populate_data( ArrayData & data, std::vector<T_> const & vec )
{
    if ( boost::is_same<T_, float>::value )
    {
        // Special case for float:
        // Reinterpret as int and populate the int32 data field
        BOOST_FOREACH( float const & x, vec )
        {
            // FIXME: Technically, type-punning like this violates the C++ standard
            data.data32.push_back( reinterpret_cast<int32_t const &>( x ) ) ;
        }
    }
    else if ( boost::is_same<T_, double>::value )
    {
        std::copy( vec.begin(), vec.end(), std::back_inserter(data.dataDouble) );
    }
    else if ( sizeof(T_) == sizeof(uint8_t) )
    {
        std::copy( vec.begin(), vec.end(), std::back_inserter(data.data8) );
    }
    else if ( sizeof(T_) == sizeof(uint16_t) )
    {
        std::copy( vec.begin(), vec.end(), std::back_inserter(data.data16) );
    }
    else if ( sizeof(T_) == sizeof(uint32_t) )
    {
        std::copy( vec.begin(), vec.end(), std::back_inserter(data.data32) );
    }
    else if ( sizeof(T_) == sizeof(uint64_t) )
    {
        std::copy( vec.begin(), vec.end(), std::back_inserter(data.data64) );
    }
}

template <typename T_>
ArrayPtr convert_to_thrift( std::vector<T_> const & vec )
{
    ArrayPtr pResult( new Array() );
    populate_description<T_>( pResult->description ) ;
    populate_data<T_>( pResult->data, vec ) ;
    return pResult ;
}

template <typename T_>
boost::shared_ptr<std::vector<T_> > convert_from_thrift( ArrayData const & dataFields )
{
    // Instead of reading the array data into some image, we'll just copy it into a big vector.
    // This should be roughly the same as a malloc + memcpy, but this function is here for completeness.

    boost::shared_ptr<std::vector<T_> > pVec( new std::vector<T_>() ) ;
    std::vector<T_> & vec = *pVec ;

    if ( boost::is_same<T_, float>::value )
    {
        // Special case for float:
        // Reinterpret as int and populate the int32 data field
        BOOST_FOREACH( int32_t const & x, dataFields.data32 )
        {
            // FIXME: Technically, type-punning like this violates the C++ standard
            vec.push_back( reinterpret_cast<float const &>( x ) ) ;
        }
    }
    else if ( boost::is_same<T_, double>::value )
    {
        std::copy( dataFields.dataDouble.begin(), dataFields.dataDouble.end(), std::back_inserter(vec) );
    }
    else if ( sizeof(T_) == sizeof(uint8_t) )
    {
        std::copy( dataFields.data8.begin(), dataFields.data8.end(), std::back_inserter(vec) );
    }
    else if ( sizeof(T_) == sizeof(uint16_t) )
    {
        std::copy( dataFields.data16.begin(), dataFields.data16.end(), std::back_inserter(vec) );
    }
    else if ( sizeof(T_) == sizeof(uint32_t) )
    {
        std::copy( dataFields.data32.begin(), dataFields.data32.end(), std::back_inserter(vec) );
    }
    else if ( sizeof(T_) == sizeof(uint64_t) )
    {
        std::copy( dataFields.data64.begin(), dataFields.data64.end(), std::back_inserter(vec) );
    }
    return pVec ;
}

struct BenchmarkStats
{
    std::string type_name ;
    float image_size_mb ;

    float message_size_mb;

    float message_creation_seconds;
    float serialization_seconds;
    float deserialization_seconds;
    float array_creation_seconds;
};

template <typename T_>
BenchmarkStats run_benchmark( size_t len )
{
    BenchmarkStats stats;

    stats.type_name = get_type_name<T_>() ;
    stats.image_size_mb = len * sizeof(T_) / 1.0e6 ;

    std::vector<T_> random_data;
    fill_random<T_>( random_data, len );

    std::vector<uint8_t> serialized_data ;

    // Write
    {
        boost::shared_ptr<TMemoryBuffer> pSendTransport( new TMemoryBuffer() ) ;
        boost::shared_ptr<TBinaryProtocol> pProtocol( new TBinaryProtocol( pSendTransport ) );

        ArrayPtr pArray ;
        Timer creation_timer ;
        {
            Timer::Token token(creation_timer) ;
            pArray = convert_to_thrift( random_data ) ;
        }
        stats.message_creation_seconds = creation_timer.seconds();

        Timer serialization_timer ;
        {
            Timer::Token token(serialization_timer) ;
            pArray->write( pProtocol.get() ) ;
            pSendTransport->flush();
        }
        stats.serialization_seconds = serialization_timer.seconds() ;

        // Copy the buffer data before we close the transport.
        uint8_t * pBuffer ;
        uint32_t bufferSize ;
        pSendTransport->getBuffer( &pBuffer, &bufferSize ) ;
        //std::cout << std::fixed << std::setprecision(3) ;
        //std::cout << "Serialized data is " << bufferSize / 1.0e6 << " MB" << std::endl ;
        stats.message_size_mb = bufferSize / 1.0e6 ;
        std::copy(pBuffer, pBuffer + bufferSize, std::back_inserter( serialized_data ) ) ;
    }

    // Read
    {
        boost::shared_ptr<TMemoryBuffer> pRcvTransport( new TMemoryBuffer( &serialized_data[0], serialized_data.size() ) ) ;
        boost::shared_ptr<TBinaryProtocol> pProtocol( new TBinaryProtocol( pRcvTransport ) );

        ArrayPtr pArray( new Array() ) ;

        Timer timer ;
        {
            Timer::Token token(timer) ;
            pArray->read( pProtocol.get() ) ;
        }
        stats.deserialization_seconds = timer.seconds();
        //std::cout << "Deserialization took: " << timer.seconds() << " seconds." << std::endl ;

        Timer conversion_timer ;
        {
            Timer::Token token(conversion_timer) ;
            boost::shared_ptr<std::vector<T_> > pVec = convert_from_thrift<T_>( pArray->data ) ;
        }
        stats.array_creation_seconds = conversion_timer.seconds() ;
    }

    return stats;
}

void print_stat_header()
{
    std::cout << "image size,"
              << "dtype,"
              << "encoded message size,"
              << "message creation time,"
              << "serialization time,"
              << "deserialization time,"
              << "array creation time"
              << std::endl ;
}

void print_stats( BenchmarkStats const & stats )
{
    std::cout << stats.image_size_mb << ','
              << stats.type_name << ','
              << stats.message_size_mb << ','
              << stats.message_creation_seconds << ','
              << stats.serialization_seconds << ','
              << stats.deserialization_seconds << ','
              << stats.array_creation_seconds
              << std::endl ;
}

// ******************************************************************
// Entry point.
// ******************************************************************
int main()
{
    std::vector<size_t> sizes = boost::assign::list_of
        (100*100)
        (100*1000)
        (1000*1000)
        (10*1000*1000)
        (100*1000*1000)
        (1000*1000*1000);

    print_stat_header();

    BOOST_FOREACH(size_t size, sizes)
    {
        print_stats( run_benchmark<uint8_t>( size ) ) ;
        print_stats( run_benchmark<int8_t>( size ) ) ;

        print_stats( run_benchmark<uint16_t>( size ) ) ;
        print_stats( run_benchmark<int16_t>( size ) ) ;

        print_stats( run_benchmark<uint32_t>( size ) ) ;
        print_stats( run_benchmark<int32_t>( size ) ) ;

        print_stats( run_benchmark<float>( size ) ) ;

        print_stats( run_benchmark<uint64_t>( size ) ) ;
        print_stats( run_benchmark<int64_t>( size ) ) ;

        print_stats( run_benchmark<double>( size ) ) ;
    }

    return 0;
}
