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
    void setSasl(std::string mechanism, std::string serverFQDN, std::string user, std::string password);
    ~Configuration();
    
private:
    infinispan::hotrod::ConfigurationBuilder* builder;
    std::string userCpy;
    std::string passwordCpy;
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
    RemoteCache(RemoteCacheManager& manager, const std::string &cacheName);
    std::vector<unsigned char>* get(const std::vector<unsigned char> &key);
    std::vector<unsigned char>* put(const std::vector<unsigned char> &key, const std::vector<unsigned char> &value);
    std::vector<unsigned char>* remove(const std::vector<unsigned char> &key);
private:
    infinispan::hotrod::RemoteCache<std::vector<unsigned char>, std::vector<unsigned char> >& cache;
};

class Util {
public:
	static std::string toString(std::vector<unsigned char>* u);
	static std::vector<unsigned char> fromString(std::string s);
};

}
