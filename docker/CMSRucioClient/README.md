Build and run like so where /tmp/x509up is a proxy generated with the DN that matches account [username]

    docker build -t cmssw/rucio_client .
    docker push cmssw/rucio_client

    docker build . -f Dockerfile.trace -t ericvaandering/rucio-trace
    docker push ericvaandering/rucio-trace

    
To run (no need to build, Eric does this occassionally):

    docker pull cmssw/rucio_client
    docker kill client; docker rm client
    docker run -d --name client cmssw/rucio_client
    docker cp ~/.globus/usercert.pem client:/tmp/usercert.pem
    docker cp ~/.globus/userkey.pem client:/tmp/userkey.pem
    docker exec -it client /bin/bash

Inside container generate a proxy and connect to the Rucio server

    chown root *.pem
    voms-proxy-init -voms cms -key userkey.pem -cert usercert.pem 
    export RUCIO_ACCOUNT=[username]
    rucio whoami

to verify it worked. (You should get feedback saying the account is correct.)

We ship the container with a number of configuration files in `/opt` which you can switch to with

    export RUCIO_HOME=/opt/rucio-[foo]/

If you want to develop the rucio client code inside the container, add ```-v /path/to/git/rucio/lib/rucio:/usr/lib/python2.7/site-packages/rucio```
to the ```docker run``` commands above. Then edit the code you checked out from GitHub in ```/path/to/git/rucio```
on your local machine and the changes will be seen inside the container.
