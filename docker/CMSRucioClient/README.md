Build and run like so where /tmp/x509up is a proxy generated with the DN that matches account [username]

    docker build -t cmssw/rucio_client .
    docker push cmssw/rucio_client
    
To run

```docker run -e "RUCIO_ACCOUNT=[username]" -v /tmp/x509up:/tmp/x509up -it cms_rucio_client /bin/bash```

or run like this to pass key and cert in

```docker run -e "RUCIO_ACCOUNT=[username]" -v ~/.globus:/tmp/globus  -it cms_rucio_client /bin/bash```

and inside the container do

```voms-proxy-init --key /tmp/globus/userkey.pem --cert /tmp/globus/usercert.pem```

to generate a proxy. Then

```rucio whoami```

to verify it worked.

If you want to develop the rucio client code inside the container, add ```-v /path/to/git/rucio/lib/rucio:/usr/lib/python2.7/site-packages/rucio```
to the ```docker run``` commands above. Then edit the code you checked out from GitHub in ```/path/to/git/rucio```
on your local machine and the changes will be seen inside the container.
