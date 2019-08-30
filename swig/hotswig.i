%module Infinispan
%include "std_string.i"
%include "std_vector.i"
%{
#include "hotrod-facade.h"
%}

// List here all the functions that allocate memory for the target language
// this directive tells SWIG to generate a wrapper to correctly deallocate memory
%newobject Infinispan::RemoteCache::get;
%newobject Infinispan::RemoteCache::put;
%newobject Infinispan::RemoteCache::remove;


%template(UCharVector) std::vector<unsigned char>;
%include "hotrod-facade.h"
