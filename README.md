# Python Client

This is a Python protoclient for connecting to an Infinispan server via Hotrod.
Focus is easyness to:

* install
* use
* understand
* extend


## Requirements

* python3
* pip3

TODO fill in

## Generate the client

It should be as easy as type:

    pip3 wheel .

or (if you need to point your OPENSSL root dir)

    OPENSSL_ROOT_DIR=/usr/local/opt/openssl
    
## Install the client

    sudo pip3 install *.whl

## Use the client
An example of client usage is in the test.py script. Run it to check if everything
went fine. You need an up and running local Infinispan server ore you'll get a
connection error.

    LD_LIBRARY_PATH=/usr/local/lib python3 test.py

The C++ native libraries are placed by the installation script in `/usr/local/lib` 
directory, so you need to tell it to the runtime loader.

## Uninstall

    sudo pip3 uninstall infinispan
