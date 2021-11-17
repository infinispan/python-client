%module Infinispan
%include "std_string.i"
%include "std_vector.i"
%include "std_shared_ptr.i"
%include "cpointer.i"
%{
#include "hotrod-facade.h"
%}

// List here all the functions that allocate memory for the target language
// this directive tells SWIG to generate a wrapper to correctly deallocate memory
%newobject Infinispan::RemoteCache::get;
%newobject Infinispan::RemoteCache::put;
%newobject Infinispan::RemoteCache::remove;

%pointer_functions(std::vector<unsigned char>, pvuc);
%template(UCharVector) std::vector<unsigned char>;
%template(UCharVectorVector) std::vector<std::vector<unsigned char> >; 
%include "hotrod-facade.h"
