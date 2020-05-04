Overview
--------
This is a docker-based test stand for CMS rucio.
It can be run anywhere with docker installed. The entrypoint is `host_run.sh`,
which sets up via docker-compose a set of 3 containers: a rucio server, a postgresql
database, and a graphite stats collector service. Then it runs a set of test routines
in `run_tests.sh`, or if `-i` is passed will open a shell in the rucio server docker
container, from where rucio daemons, probes, and client can be run interactively or
to develop additional tests/scripts. On first run, the CMS current rucio version
is checked out via git into `rucio` in this directory, and the rucio `probes` is also
checked out. All files are mounted into the docker container so that they can be modified
while running the container. In some cases, a restart of httpd and flush of memcache
may be needed after modifying, so the `setup.sh` script can be run again within the 
container.

pgAdmin works well to interact with the database (localhost:5432/rucio, pass `secret`)

Probe development
-----------------
Suggested workflow:

 - Start the cluster with `host_run.sh -i`
 - Run `run_tests.sh` in the container to generate some activity in rucio
 - Make additional DIDs/rules/etc.
 - Edit a probe in `probes/common/`
 - Run a probe (e.g. `/opt/rucio/probes/check_stuck_rules`)
 - Look at the graphite server at http://localhost for the resulting metrics (e.g. the
   above would create `stats.gauges.docker.judge.stuck_rules_with_missing_source_replica`)

The probes eventually are built into a container with https://github.com/dmwm/CMSKubernetes/blob/master/docker/rucio-probes/Dockerfile
