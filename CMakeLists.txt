CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
project (Foo)

include (ExternalProject)

############################################################################
# Check if BUILDEM_DIR has already been assigned.  If not, create a default.
set (BUILDEM_DIR "None" CACHE TYPE STRING)

if (${BUILDEM_DIR} STREQUAL "None")
    message (FATAL_ERROR "ERROR: BuildEM build directory (for all downloads & builds) should be specified via -D BUILDEM_DIR=<path> on cmake command line.")
endif ()


############################################################################
# Download and install buildem, if it isn't already in BUILDEM_DIR.
set (BUILDEM_REPO_DIR ${BUILDEM_DIR}/src/buildem)
if (NOT EXISTS ${BUILDEM_REPO_DIR}/python.cmake)
    message ("Installing buildem repo...")
    ExternalProject_Add(buildem
        PREFIX              ${BUILDEM_DIR}
        GIT_REPOSITORY      https://github.com/janelia-flyem/buildem.git
        #GIT_TAG            python3  # Example of tagged branch (see doc)
        UPDATE_COMMAND      ""
        PATCH_COMMAND       ""
        CONFIGURE_COMMAND   "" 
        BUILD_COMMAND       ""
        BUILD_IN_SOURCE     1
        INSTALL_COMMAND     ""
    )
    message ("\n**********************************************************\n")
    message ("\nAfter running make, you must re-run the cmake command once")
    message ("buildem has been downloaded!\n")
    message ("\n***********************************************************\n")
else ()
############################################################################

    # Use modules from the downloaded buildem
    set (CMAKE_MODULE_PATH ${BUILDEM_REPO_DIR})
    message("Using cmake modules from ${BUILDEM_REPO_DIR}")

    include (boost)
    include (thrift)

    add_custom_target (AppDependencies ALL
        DEPENDS ${APP_DEPENDENCIES}
        )

    ##########################
    ## Thrift auto-gen Python
    add_custom_target(gen-thrift-py
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
      COMMAND  ${BUILDEM_ENV_STRING} thrift --gen py -o py dvidmsg.thrift
      DEPENDS dvidmsg.thrift
      COMMENT "Autogenerating thrift Python files..."
      VERBATIM )

    ##########################
    ## Thrift auto-gen C++
    add_custom_command(
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
      OUTPUT ${CMAKE_SOURCE_DIR}/cpp/gen-cpp/dvidmsg_constants.cpp
             ${CMAKE_SOURCE_DIR}/cpp/gen-cpp/dvidmsg_types.cpp
             ${CMAKE_SOURCE_DIR}/cpp/gen-cpp/BenchmarkService.cpp
      COMMAND ${BUILDEM_ENV_STRING} thrift --gen cpp -o cpp dvidmsg.thrift
      DEPENDS ${boost_NAME} ${thrift_NAME} dvidmsg.thrift
      COMMENT "Autogenerating thrift C++ files..."
      VERBATIM )

    ##########################
    ## Serialization Benchmark
    add_executable(dvidmsg-cpp-benchmark cpp/serialization_benchmark.cpp
                                         cpp/timer.cpp
                                         cpp/gen-cpp/BenchmarkService.cpp 
                                         cpp/gen-cpp/dvidmsg_constants.cpp 
                                         cpp/gen-cpp/dvidmsg_types.cpp )
    #add_dependencies(dvidmsg-cpp-benchmark AppDependencies)
    target_link_libraries(dvidmsg-cpp-benchmark ${thrift_LIBRARIES} ${boost_LIBS})

    set_target_properties( dvidmsg-cpp-benchmark
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "./bin"
        # COMPILE_FLAGS "-std=c++11" # boost-1.51 chrono lib doesn't compile under c++11 
    )

    #################
    ## Example Server
    #add_executable(dvidmsg-cpp-server gen-cpp/BenchmarkService_server.skeleton.cpp 
    #                                  gen-cpp/BenchmarkService.cpp 
    #                                  gen-cpp/dvidmsg_constants.cpp 
    #                                  gen-cpp/dvidmsg_types.cpp)
    #add_dependencies(dvidmsg-cpp-server AppDependencies)
    #target_link_libraries(dvidmsg-cpp-server ${thrift_LIBRARIES} ${boost_LIBS})
#
#    set_target_properties( dvidmsg-cpp-server
#        PROPERTIES
#        RUNTIME_OUTPUT_DIRECTORY "./bin"
#        COMPILE_FLAGS "-std=c++11"
#    )
    
    
############################################################################
endif()
