%include <std_common.i>

%{
#include <set>
#include <algorithm>
#include <stdexcept>
%}

// exported class

namespace std {

    template<class V > class set {
      public:
        typedef size_t size_type;
        typedef V value_type;
        set();
        set(const set< V > &);

        unsigned int size() const;
        bool empty() const;
        void clear();
        %extend {
            const V& get(const V& key) throw (std::out_of_range) {
                std::set< V >::iterator i = self->find(key);
                if (i != self->end())
                    return *i;
                else
                    throw std::out_of_range("key not found");
            }
            void insert(const V& key) { // Do NOT call this function 'set' !
                self->insert(key);
            }
            void del(const V& key) throw (std::out_of_range) {
                std::set< V >::iterator i = self->find(key);
                if (i != self->end())
                    self->erase(i);
                else
                    throw std::out_of_range("key not found");
            }
            bool has_key(const V& key) {
                std::set< V >::iterator i = self->find(key);
                return i != self->end();
            }
        }
    };

}
