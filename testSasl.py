from Infinispan import Infinispan
conf=Infinispan.Configuration()
conf.addServer("localhost",11222)
conf.setProtocol("2.4")
conf.setSasl("PLAIN", "node0", "writer", "somePassword")
manager=Infinispan.RemoteCacheManager(conf)
manager.start()
key=Infinispan.UCharVector()
key.push_back(56)
value=Infinispan.UCharVector()
value.push_back(8)
cache=Infinispan.RemoteCache(manager, "myCache")
cache.put(key,value)
res=cache.get(key)
print (res.pop())
manager.stop()
