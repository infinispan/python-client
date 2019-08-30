#include <vector>
#include <string>

namespace infinispan {
namespace hotrod {
class RemoteCacheManager;
template <class K, class V> class RemoteCache;
class ConfigurationBuilder;
}
}
namespace Infinispan {

class RemoteCacheManager;
class RemoteCache;

class Configuration {
public:
    Configuration();
    void addServer(std::string host, unsigned short post);
    void setProtocol(std::string);
    void build();
    ~Configuration();
private:
    infinispan::hotrod::ConfigurationBuilder* builder;
friend RemoteCacheManager;
};

class RemoteCacheManager {
public:
    RemoteCacheManager(Configuration& configuration);
    void start();
    void stop();
    ~RemoteCacheManager();
private:
    infinispan::hotrod::RemoteCacheManager* manager;
friend RemoteCache;
};

class RemoteCache {
public:
    RemoteCache(RemoteCacheManager& manager);
    std::vector<unsigned char>* get(const std::vector<unsigned char> &key);
    std::vector<unsigned char>* put(const std::vector<unsigned char> &key, const std::vector<unsigned char> &value);
    std::vector<unsigned char>* remove(const std::vector<unsigned char> &key);
private:
    infinispan::hotrod::RemoteCache<std::vector<unsigned char>, std::vector<unsigned char> >& cache;
};

}
