from Infinispan import Infinispan

def toString(ucVec):
    if ucVec==None:
        return None
    return Infinispan.Util.toString(ucVec)

def fromString(str):
    return Infinispan.Util.fromString(str)
