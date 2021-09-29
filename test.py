from Infinispan import Infinispan
conf=Infinispan.Configuration()
conf.addServer("localhost",11222)
conf.setProtocol("2.4")
manager=Infinispan.RemoteCacheManager(conf)
manager.start()
key=Infinispan.UCharVector()
# Setting key to '8'
key.push_back(56)
value=Infinispan.UCharVector()
# Setting key to '1'
value.push_back(49)
cache=Infinispan.RemoteCache(manager,"myCache")
cache.put(key,value)
res=cache.get(key)
print (res.pop())
keyNotFound=Infinispan.UCharVector()
keyNotFound.push_back(57)
resNotFound=cache.get(keyNotFound)
if resNotFound==None:
    print("Not found")
manager.stop()
