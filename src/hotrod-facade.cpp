#define HR_PROTO_EXPORT
#include "hotrod-facade.h"
#include "infinispan/hotrod/ConfigurationBuilder.h"
#include "infinispan/hotrod/ServerConfigurationBuilder.h"
#include "infinispan/hotrod/Marshaller.h"
#include "infinispan/hotrod/RemoteCache.h"
#include <infinispan/hotrod/RemoteCacheManager.h>
#include <sasl/saslplug.h>


static const char *simple_data; // plain

#if !defined _WIN32 && !defined _WIN64
static char realm_data[] = "applicationRealm";
#else
static char realm_data[] = "ApplicationRealm";
#endif

static int simple(void* /* context */, int id, const char **result, unsigned *len) {
    *result = simple_data;
    if (len)
        *len = strlen(simple_data);
    return SASL_OK;
}

static int getUser(void*  context, int id, const char **result, unsigned *len) {
    *result = (char*) context;
    if (len)
        *len = strlen((char*)context);
    return SASL_OK;
}

static int getrealm(void* /* context */, int id, const char **result, unsigned *len) {
    *result = realm_data;
    if (len)
        *len = strlen(realm_data);
    return SASL_OK;
}

#define PLUGINDIR "/usr/lib64/sasl2"

static int getpath(void *context, const char ** path) {
    if (!path)
        return SASL_BADPARAM;
    *path = PLUGINDIR;
    return SASL_OK;
}

static const char *secret_data;

static int getsecret(void* /* conn */, void* /* context */, int id, sasl_secret_t **psecret) {
    size_t len;
    static sasl_secret_t *x;
    len = strlen(secret_data);

    x = (sasl_secret_t *) realloc(x, sizeof(sasl_secret_t) + len);

    x->len = len;
    strcpy((char *) x->data, secret_data);

    *psecret = x;
    return SASL_OK;
}

static int getPassword(void* /* conn */, void*  context, int id, sasl_secret_t **psecret) {
    size_t len;
    static sasl_secret_t *x;
    len = strlen((char*)context);

    x = (sasl_secret_t *) realloc(x, sizeof(sasl_secret_t) + len);

    x->len = len;
    strcpy((char *) x->data, (char*)context);

    *psecret = x;
    return SASL_OK;
}



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

void Configuration::setSasl(std::string mechanism, std::string serverFQDN, std::string user, std::string password) {
	// BEWARE this implementation copies user and password in memory
        userCpy = user;
        passwordCpy = password;
        std::vector<sasl_callback_t> callbackHandler { { SASL_CB_USER, (sasl_callback_ft) &getUser, (void**)userCpy.data() }, {
                    SASL_CB_AUTHNAME, (sasl_callback_ft) &getUser, (void**)userCpy.data() }, { SASL_CB_PASS, (sasl_callback_ft) &getPassword, (void**)passwordCpy.data() }, {
                    SASL_CB_GETREALM, (sasl_callback_ft) &getrealm, NULL }, { SASL_CB_GETPATH, (sasl_callback_ft) &getpath, NULL }, {
                    SASL_CB_LIST_END, NULL, NULL } };
        this->builder->security().authentication().saslMechanism(mechanism).serverFQDN(
                serverFQDN).callbackHandler(callbackHandler).enable();
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
RemoteCache::RemoteCache(RemoteCacheManager& m, const std::string &cacheName) : cache(m.manager->getCache<std::vector<unsigned char>
                                                       , std::vector<unsigned char> >(new IdentityMarshaller()
                                                       , &IdentityMarshaller::destroy, new IdentityMarshaller()
                                                       , &IdentityMarshaller::destroy, cacheName)) {
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

std::string Util::toString(std::vector<unsigned char>* u) {
	return std::string(u->data(), u->data()+u->size());
}
std::vector<unsigned char> Util::fromString(std::string s) {
	return std::vector<unsigned char>(s.data(), s.data()+s.size());
}

}
