%{
#include <future>
%}
namespace std {
  template <typename Type>
  class future {
      public:
      Type get();
  };
}
