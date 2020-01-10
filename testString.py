from Infinispan import Infinispan
from Ispn import Util
conf=Infinispan.Configuration()
conf.addServer("localhost",11222)
conf.setProtocol("2.4")
manager=Infinispan.RemoteCacheManager(conf)
manager.start()
key="stringKey"
value="stringValue"
cache=Infinispan.RemoteCache(manager,"default")
ucKey=Util.fromString(key)
ucValue=Util.fromString(value)
cache.put(ucKey, ucValue)
ucRes=cache.get(ucKey)
res= Util.toString(ucRes);
print (res)
keyNotFound="notFound"
ucKNF= Util.fromString(keyNotFound)
resNotFound=cache.get(ucKNF)
if resNotFound==None:
    print("Not found")
manager.stop()
