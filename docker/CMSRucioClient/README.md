Build and run like so where /tmp/x509up is a proxy generated with the DN that matches account [username]

docker build -t cms_rucio_client .

docker run -e "RUCIO_ACCOUNT=[username]" -v /tmp/x509up:/tmp/x509up -it cms_rucio_client /bin/bash
