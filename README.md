# Infinispan's Native Python Client

## Introduction
This is a native Python client implementation of [Infinispan's Hot Rod
client/server protocol] [1], which allows Python applications to connect and
query or store data in Infinispan remote caches based on Hot Rod.

In order to connect to Hot Rod based Infinispan remote caches, it's necessary
that a server is up and running. To find out more on how to start an Infinispan
remote cache server, follow the instructions on [using the Infinispan Hot Rod
server] [2].

## Requirements
This client requires Python 2.6 or higher.

## Installation
### Via distutils
Installing the python client is a very simple process. Once you've downloaded
the source bundle for the client, execute the following:

    python setup.py install --record files.txt

### Via python eggs
To install the Infinispan python client via a python egg, you can access the
PyPI URL directly via:

    easy_install ...

Or you can build the egg from the root of the source and install it
immediately:

    python setup.py bdist_egg
    easy_install dist/infinispan-*.egg

## Testing the client
Once you've installed the client, testing it is very easy. First of all
download one of the Infinispan *-bin.zip distributions and start a Hot Rod
server using default values. Then, execute a simple script such as this:

    import sys
    from infinispan.remotecache import RemoteCache

    def main():
      remote_cache = RemoteCache()
      remote_cache.put("Name", "Galder")
      name = remote_cache.get("Name")
      print "My name is %s" % name
      return

    if __name__ == "__main__":
      sys.exit(main())

When you this script, the output should say:

> My name is Galder

## Uninstallation
### If installed via distutils
If you executed the suggested installation a command, a file called 'files.txt'
would have been generated. This file can later be used to uninstall the client
using the following command:

    cat files.txt | xargs rm -rf
### If installed via python eggs
Simply execute:

    easy_install -m infinispan

## Versions
* 1.0.0b1 - This version provides a client intelligence level 1 implementation
for the [Infinispan Hot Rod protocol] [1]. Please read the specification in
order to find out more.

[1]: http://community.jboss.org/docs/DOC-14421
[2]: http://community.jboss.org/docs/DOC-15093

