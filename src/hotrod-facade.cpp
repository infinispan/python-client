#define HR_PROTO_EXPORT
#include "hotrod-facade.h"
#include "infinispan/hotrod/ConfigurationBuilder.h"
#include "infinispan/hotrod/ServerConfigurationBuilder.h"
#include "infinispan/hotrod/Marshaller.h"
#include "infinispan/hotrod/RemoteCache.h"
#include <infinispan/hotrod/RemoteCacheManager.h>

namespace Infinispan
{
class IdentityMarshaller : public infinispan::hotrod::Marshaller<std::vector<unsigned char> > {
public:
    virtual void marshall(const std::vector<unsigned char>& in, std::vector<char>& out) {
        out = std::vector<char>(in.begin(),in.end());
    }
    virtual std::vector<unsigned char>* unmarshall(const std::vector<char>& in) {
        return new std::vector<unsigned char>(in.begin(),in.end());
    }
    static void destroy(infinispan::hotrod::Marshaller<std::vector<unsigned char> > *marshaller) { delete marshaller; }
};

Configuration::Configuration() : builder(new infinispan::hotrod::ConfigurationBuilder()) {}
Configuration::~Configuration() { delete builder; }
void Configuration::addServer(std::string host, unsigned short port) {
    this->builder->addServer().host(host).port(port);
}

void Configuration::setProtocol(std::string protocol) {
    this->builder->protocolVersion(protocol);
}

void Configuration::build()
{
    this->builder->build();
}

RemoteCacheManager::RemoteCacheManager(Configuration &c) : manager(new infinispan::hotrod::RemoteCacheManager(c.builder->build(), false)){
}

void RemoteCacheManager::start() {
    manager->start();
}

void RemoteCacheManager::stop() {
    manager->stop();
}

RemoteCacheManager::~RemoteCacheManager() { delete manager; }

RemoteCache::RemoteCache(RemoteCacheManager& m) : cache(m.manager->getCache<std::vector<unsigned char>
                                                       , std::vector<unsigned char> >(new IdentityMarshaller()
                                                       , &IdentityMarshaller::destroy, new IdentityMarshaller()
                                                       , &IdentityMarshaller::destroy)) {
}

std::vector<unsigned char>* RemoteCache::get(const std::vector<unsigned char> &key) {
    return cache.get(key);
}

std::vector<unsigned char>* RemoteCache::put(const std::vector<unsigned char> &key, const std::vector<unsigned char> &value) {
    return cache.put(key,value);
}
std::vector<unsigned char>* RemoteCache::remove(const std::vector<unsigned char> &key) {
    return cache.remove(key);
}
}
