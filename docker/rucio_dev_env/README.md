Build and run like so where /tmp/x509up is a proxy generated with the DN that matches account [username]

```
docker build -t cmssw/rucio_dev_env .
docker push cmssw/rucio_dev_env
```

If you want to develop the rucio client code inside the container, add ```-v /path/to/git/rucio/lib/rucio:/usr/lib/python2.7/site-packages/rucio```
to the ```docker run``` command. Then edit the code you checked out from GitHub in ```/path/to/git/rucio```
on your local machine and the changes will be seen inside the container.

A git copy of the rucio code is also in /root/rucio
