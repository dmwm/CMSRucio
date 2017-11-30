Build and run like so where /tmp/x509up is a proxy generated with the DN that matches account [username]

docker build -t cms_rucio_client .

To run

docker run -e "RUCIO_ACCOUNT=[username]" -v /tmp/x509up:/tmp/x509up -it cms_rucio_client /bin/bash

or 

docker run -e "RUCIO_ACCOUNT=[username]" -v ~/.globus:/.globus -it cms_rucio_client /bin/bash

and use voms-proxy-init to create a proxy inside the container.
