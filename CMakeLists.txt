cmake_minimum_required (VERSION 2.8.11)
project (hotswig)

set(TARGET_LANG python CACHE STRING "Target language. Defaulted to python")

find_package(SWIG REQUIRED)
include(${SWIG_USE_FILE})

##### Some setting language specifics

#load the package that SWIG uses to generate Python
FIND_PACKAGE(PythonInterp 3)
FIND_PACKAGE(PythonLibs 3)
include_directories(${PYTHON_INCLUDE_DIRS} /usr/local/opt/protobuf/include)

##### End of some setting language specifics


include_directories(include build/cpp-client/src/cppclient/include build/cpp-client/src/cppclient-build ${TARGET_LANG_INCLUDE})
set_property(SOURCE swig/hotswig.i PROPERTY CPLUSPLUS ON)
swig_add_library(Infinispan 
	TYPE SHARED
	LANGUAGE ${TARGET_LANG}
	OUTPUT_DIR ${PROJECT_SOURCE_DIR}/Infinispan
	SOURCES swig/hotswig.i src/hotrod-facade.cpp)
swig_link_libraries (Infinispan hotrod ${PYTHON_LIBRARIES})

include(ExternalProject)
ExternalProject_Add(cppclient
  GIT_REPOSITORY    https://github.com/infinispan/cpp-client.git
  BUILD_COMMAND make hotrod 
  INSTALL_COMMAND ""
  PREFIX ${PROJECT_SOURCE_DIR}/build/cpp-client
  CMAKE_ARGS -DNOENABLE_VALGRIND=$ENV{NOENABLE_VALGRIND} -DNOENABLE_INTERNAL_TESTING=$ENV{NOENABLE_INTERNAL_TESTING} -D=NOENABLE_SWIG_TESTING=$ENV{NOENABLE_SWIG_TESTING} -DOPENSSL_ROOT_DIR=$ENV{OPENSSL_ROOT_DIR}
)

add_library(hotrod SHARED IMPORTED)

ExternalProject_Get_Property(cppclient binary_dir)
set_property(TARGET hotrod PROPERTY IMPORTED_LOCATION ${binary_dir}/${CMAKE_SHARED_LIBRARY_PREFIX}hotrod${CMAKE_SHARED_LIBRARY_SUFFIX})
