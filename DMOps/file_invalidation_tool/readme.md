# File Invalidation Tool for Rucio and DBS
## Overview

This guide outlines the steps to run the file invalidation tool for Rucio and DBS using Docker image. The tool assists in invalidating specific files, datasets or containers within these systems, to ensure data consistency. Additionally, it has a running mode to check the integrity of files in a given RSE(checksum validation), and invalidate the corrupted replicas. Finally, the tool can also be used to invalidate all files in a given site.

## Prerequisites, Folder Structure and tool input

### Tool Input

The tool has 5 running modes.  It's important that your cert and key (decrypted) have enough permissions to invalidate on DBS and declare replicas as corrupted on Rucio, additionally to this it they require the following inputs and parameters:

| Running Mode | Description | Tool Mode | Input File | Params | Auth Requirements |
| ----------- | ----------- | ----------- | ----------- | ----------- | ----------- |
| Global Invalidation | Invalidate all files from received files, datasets or containers list on Rucio and DBS | `global` | `<filename>.txt`: txt file containing list of files, datasets or containers | `--reason <reason>`: comment for invalidation<br>`--dry-run`(**optional**): Simulate the execution without actually performing the file invalidation<br>`--erase-mode`(**optional**): Erase empty DIDs | `./certs/usercert.pem`<br>`./certs/userkey.pem`<br>`./secrets/dmtops.keytab`|
| DBS Invalidation | Invalidate all files from received files, datasets or containers list only on DBS | `only-dbs` | `<filename>.txt`: txt file containing list of files, datasets or containers | `--reason <reason>`: comment for invalidation<br>`--dry-run`(**optional**): Simulate the execution without actually performing the file invalidation<br>`--erase-mode`(**optional**): Erase empty DIDs | `./certs/usercert.pem`<br>`./certs/userkey.pem`<br>`./secrets/dmtops.keytab`|
| Rucio Invalidation | Invalidate all files from received files, datasets or containers list only on Rucio | `only-rucio` | `<filename>.txt`: txt file containing list of files, datasets or containers | `--reason <reason>`: comment for invalidation<br>`--dry-run`(**optional**): Simulate the execution without actually performing the file invalidation<br>`--erase-mode`(**optional**): Erase empty DIDs | `./certs/usercert.pem`<br>`./certs/userkey.pem`<br>`./secrets/dmtops.keytab`|
| Integrity Validation | Validate integrity of files in the given RSE | `integrity-validation` | `<filename>.csv`: csv file containing list of files and RSE [FILENAME,RSE_EXPRESSION] | `--dry-run`(**optional**): Simulate the execution without actually performing the file invalidation in case of being corrupted | `./certs/usercert.pem`<br>`./certs/userkey.pem`|
| Site Invalidation | Invalidate in Rucio all files from received list at a specific site | `site-invalidation` | `<filename>.txt`: txt file containing list of files, datasets or containers | `--rse <rse>`: RSE to invalidate at<br>`--reason <reason>`: comment for invalidation<br>`--dry-run`(**optional**): Simulate the execution without actually performing the file invalidation | `./certs/usercert.pem`<br>`./certs/userkey.pem`<br>`./secrets/dmtops.keytab`|

> **Note:** The userkey.pem should be decrypted.

??? Example
    **USERKEY decryption**
    `openssl rsa -in <encrypted_userkey> -out userkey.pem`

    You would be asked to enter the password.

??? Info
    **Checksum Validation Mode**

    Some files could be heavy and may lead to exceed your lxplus quota. In case of seeing this error move your working directory to `/eos/user/<first_username_letter>/<username>/` directory.
    ```Bash
    gfal-copy error: 122 (Disk quota exceeded) - errno reported by local system call Disk quota exceeded
    ```

### Environment

This script is thought to be run on **lxplus** or CERN server with access to `registry.cern.ch` and `/cvmfs/` directory.

Setting all together, the working directory structure can change a bit, but it should look like this:
working_directory/
├── dids.txt / replicas_validation.csv
├── certs/
│   ├── usercert.pem
│   └── userkey.pem

## Run File Invalidation tool

### 1. CERN Registry Authentication

1. Visit [cern registry](https://registry.cern.ch/).
2. Login via OIDC Provider.
3. Click on your username located in the top right.
4. Click on **User Profile**
5. Copy the **CLI Secret**, it will be used in the next step.

### 2. Login into CERN Registry
```Bash
docker login registry.cern.ch -u <username>
```
- `docker login`: Logs in to the Docker registry.
- `registry.cern.ch`: CERN registry URL.
- `-u <username>`: CERN registry username.

It will ask you to enter your password. **Enter your CLI Secret.**

### 3. Run the container

```Bash
docker run -P \
  -v "$(pwd)/<input_file>:/input/<input_file>" \
  -v "$(pwd)/certs:/certs" \
  [-v "$(pwd)/secrets:/secrets" \]
  --mount type=bind,source=/cvmfs/,target=/cvmfs/,readonly \
  --network host --rm registry.cern.ch/cmsrucio/file_invalidation_tool [Tool_Mode_Options]
```
- `docker run`: Executes a Docker container.
- `-P`: Publishes all exposed ports to the host interfaces.
- Volumes mounted:
  - `-v "$(pwd)/<input_file>:/input/<input_file>"`: Mounts the containers_inv.txt file from the host to /input/dids.txt within the container.
  - `-v "$(pwd)/certs:/certs"`: Mounts the certs directory from the host to /certs within the container. It must contain the usercert.pem and userkey.pem.
  - `-v "$(pwd)/secrets:/secrets"`: Mounts the secrets directory from the host to /secrets within the container. It must contain the keytab file.
  - `--mount type=bind,source=/cvmfs/,target=/cvmfs/,readonly`: Binds the /cvmfs/ directory on the host as read-only within the container.
- `--network host`: Uses the host's network stack within the container.
- `--rm`: Automatically removes the container when it exits.
- `registry.cern.ch/cmsrucio/file_invalidation_tool`: Name of the Docker image to run.

??? Example

    ```Bash
    docker run -P \
      -v "$(pwd)/<input_file>:/input/<input_file>.txt" \
      -v "$(pwd)/certs:/certs" \
      -v "$(pwd)/secrets:/secrets" \
      --mount type=bind,source=/cvmfs/,target=/cvmfs/,readonly \
      --network host --rm registry.cern.ch/cmsrucio/file_invalidation_tool [global | only-dbs | only-rucio] --reason <reason>
    ```

    ```Bash
    docker run -P \
      -v "$(pwd)/<input_file>:/input/<input_file>.csv" \
      -v "$(pwd)/certs:/certs" \
      --mount type=bind,source=/cvmfs/,target=/cvmfs/,readonly \
      --network host --rm registry.cern.ch/cmsrucio/file_invalidation_tool integrity-validation
    ```

    ```Bash
    docker run -P \
      -v "$(pwd)/<input_file>:/input/<input_file>.txt" \
      -v "$(pwd)/certs:/certs" \
      -v "$(pwd)/secrets:/secrets" \
      --mount type=bind,source=/cvmfs/,target=/cvmfs/,readonly \
      --network host --rm registry.cern.ch/cmsrucio/file_invalidation_tool site-invalidation --rse <rse>  --reason <reason>
    ```
## Additional Notes

- The tool's output will provide details about the invalidation process.
- User Authorization: Ensure you have the necessary permissions to invalidate on DBS.
  - The provided certificates will be used for DBS invalidation, in case of authorization errors, rucio invalidation will not be executed.
  - Rucio Invalidation will be done using the  the dmtops certificate and transfer_ops account since many users will not have permissions to develop this operation.