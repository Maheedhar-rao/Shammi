import pickle
import time
from .Bases import Singleton
from .TermOutput import msg

class RequestCache(metaclass=Singleton):
    def __init__(self, cacheFile=None):
        self.cacheFile = cacheFile
        try:
            self.cache = pickle.load(open(self.cacheFile,"rb"))
        except FileNotFoundError:
            msg.info("Started new request cache")
            self.cache = {}

    def writeCacheFile(self):
        pickle.dump(self.cache, open(self.cacheFile,"wb"))

def cachedRequest(name=None):
    def _cachedRequest(fn):
        def wrapper(*args, **kwargs):
            rc = RequestCache()
            if name not in rc.cache:
                rc.cache[name] = dict()
            cache = rc.cache[name]

            if 'cache_key' in kwargs:
                if kwargs['cache_key'] is None:
                    raise RuntimeError("cache_key required")
                x = kwargs['cache_key']
            else:
                x = args[0]
            
            if x not in cache:
                msg.debug("%s cache miss", name)
                time.sleep(0.4)
                cache[x] = fn(*args, **kwargs)
                rc.writeCacheFile()
            return cache[x]
        return wrapper
    return _cachedRequest
