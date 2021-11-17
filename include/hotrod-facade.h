#include <vector>
#include <string>
#include <memory>

namespace infinispan {
namespace hotrod {
class RemoteCacheManager;
template <class K, class V> class RemoteCache;
class ConfigurationBuilder;
class RemoteCacheManagerAdmin;
}
}
namespace Infinispan {

class RemoteCacheManagerAdmin;
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
    std::string realmCpy;
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
friend RemoteCacheManagerAdmin;
};

class RemoteCacheManagerAdmin {
public:
    RemoteCacheManagerAdmin(RemoteCacheManager& rcm);
    RemoteCache createCache(const std::string name, std::string model);
    RemoteCache createCacheWithXml(const std::string name, std::string conf);
    RemoteCache getOrCreateCache(const std::string name, std::string model);
    RemoteCache getOrCreateCacheWithXml(const std::string name, std::string conf);
private:
    RemoteCacheManager& rcm;
    std::shared_ptr<infinispan::hotrod::RemoteCacheManagerAdmin> admin;
};

class RemoteCache {
public:
    RemoteCache(RemoteCacheManager& manager);
    RemoteCache(RemoteCacheManager& manager, const std::string &cacheName);
    std::vector<unsigned char>* get(const std::vector<unsigned char> &key);
    std::vector<unsigned char>* put(const std::vector<unsigned char> &key, const std::vector<unsigned char> &value);
    std::vector<unsigned char>* remove(const std::vector<unsigned char> &key);
    std::vector<std::vector<unsigned char> > keys();
    bool containsKey(const std::vector<unsigned char> &key);
private:
    infinispan::hotrod::RemoteCache<std::vector<unsigned char>, std::vector<unsigned char> >& cache;
};

class Util {
public:
	static std::string toString(std::vector<unsigned char> u);
	static std::vector<unsigned char> fromString(std::string s);
};

}
