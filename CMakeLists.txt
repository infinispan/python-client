cmake_minimum_required (VERSION 2.8.11)
project (hotswig)

set(PREBUILT_DIR ${PROJECT_SOURCE_DIR}/prebuilt CACHE STRING "Hotrod C++ install dir. Defaulted to ./prebuilt")
set(TARGET_LANG python CACHE STRING "Target language. Defaulted to python")

find_package(SWIG REQUIRED)
include(${SWIG_USE_FILE})

##### Some setting language specifics

# Python
if (${TARGET_LANG} STREQUAL "python")
#load the package that SWIG uses to generate Python
FIND_PACKAGE(PythonInterp 3)
FIND_PACKAGE(PythonLibs 3)
#point to python headers
include_directories(${PYTHON_INCLUDE_DIRS})
endif (${TARGET_LANG} STREQUAL "python")

#Octave
if (${TARGET_LANG} STREQUAL "octave")
set(TARGET_LANG_INCLUDE "/usr/include/octave-4.2.1/")
endif (${TARGET_LANG} STREQUAL "octave")

##### End of some setting language specifics


include_directories(include build/cpp-client/src/cppclient/include build/cpp-client/src/cppclient-build ${TARGET_LANG_INCLUDE})
link_directories(${PREBUILT_DIR}/usr/lib)
set_property(SOURCE swig/hotswig.i PROPERTY CPLUSPLUS ON)
swig_add_library(Infinispan 
	LANGUAGE ${TARGET_LANG}
	OUTPUT_DIR ${PROJECT_SOURCE_DIR}/Infinispan
	SOURCES swig/hotswig.i src/hotrod-facade.cpp)
swig_link_libraries (Infinispan hotrod)

include(ExternalProject)
ExternalProject_Add(cppclient
  GIT_REPOSITORY    git@github.com:infinispan/cpp-client.git
  BUILD_COMMAND make hotrod 
  INSTALL_COMMAND ""
  PREFIX ${PROJECT_SOURCE_DIR}/build/cpp-client
)

add_library(hotrod SHARED IMPORTED)

ExternalProject_Get_Property(cppclient binary_dir)
set_property(TARGET hotrod PROPERTY IMPORTED_LOCATION ${binary_dir}/libhotrod.so)
