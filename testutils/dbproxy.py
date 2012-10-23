import inspect
import types
from threading import local

__all__ = ('DatabaseProxy',)


class DatabaseProxy(local):
    # List of attributes that shouldn't be proxied.
    no_proxy = ('queries', 'proxy', '_proxied_conn')

    def __init__(self, connection, alias):
        self._proxied_conn = connection
        self.queries = []
        self.proxy = alias

    def __getattribute__(self, name):
        if name in object.__getattribute__(self, 'no_proxy'):
            return object.__getattribute__(self, name)

        # Grab the connection we want to proxy to, and look up attribute
        # against the connection instead.  We rebind instance methods to
        # our proxy, if necessary.
        proxied_conn = object.__getattribute__(self, '_proxied_conn')
        retval = getattr(proxied_conn, name)
        if type(retval) is types.MethodType:
            # Get unbound method from class.
            retval = getattr(proxied_conn.__class__, name)
            if hasattr(retval, 'im_func'):
                retval = getattr(retval, 'im_func')
        if callable(retval) and not inspect.isclass(retval):
            retval = types.MethodType(retval, self, DatabaseProxy)
        return retval

    def __setattr__(self, name, value):
        if name in object.__getattribute__(self, 'no_proxy'):
            object.__getattribute__(self, '__dict__')[name] = value
        else:
            proxied_conn = object.__getattribute__(self, '_proxied_conn')
            proxied_conn.__dict__[name] = value
