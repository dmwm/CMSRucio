This directory contains temporary helper scripts that we can share for certain DM tasks. 
They are not part of the main codebase and are not intended to be used in production. They are just here to help us with some tasks that we need to do once in a while.

It is useful to specify readme and dates for each helper script.
After which one can delete the script from this directory.

## Helper scripts

### test_http_tape_rest_api.py

This is to test HTTP REST API readiness for all our Tapes.

1. Create a Jira ticket for the site. 
2. Temporarily turn off `update_from_json=False` for the main Tape RSE.
    - Switch it on for the `*_Tape_Test` RSE.
3. Coordinate with site support and get the site to add the webdav endpoint to the siteconf
4. Once the protocol is updated for the Tape_Test RSE by the sync jobs. Run the helper script
    - The script will delete the other protocols and set update from json to false. 
    - It will also create a rule to and from the Tape_Test RSE depending on the mode.
    - It uses the `Debug` activity which can be used as filter in CMS FTS Dashboard.
    - The immediate monitoring source should however be the FTS UI
       - Filter on the destination se and look at individual transfers

5. For a Write Test: `python3 test_http_tape_rest_api.py --rse RSENAME --mode write`
    ```
    optionally:
        --scope SCOPE
        --name NAME
        can be specified if the default scope and name are not desired.
    ```
6. For a Read Test `python3 test_http_tape_rest_api.py --readtorse RSENAME --mode read --ruleid RULEID`
    ```    
    where RULEID is the rule id of the file that was written in the write test.
    optionally:
        --readtorse RSE
        can be specified if the default destination RSE is not desired.
    ```
7. Monitor FTS transfer and rule for errors
   - Get them resolved with ticket to sites via GGUS
   - Keep site support in loop for any help

8. Feel free to delete and suspend rules and modify the script for you convinience.

9. Finally, set update_from_json to true for both the main and test _Tape RSEs.

PS: Start with a small functional test transfer, the default dataset is a good example.
If it works, then try with a larger dataset for stress testing.


---- 

Adding scripts for retrieving file locality information on buffer and tape.

```bash
curl -X POST --cert $X509_USER_PROXY --key $X509_USER_PROXY --capath /etc/grid-security/certificates https://<hostname>:<port>/api/v1/tape/archiveinfo> \
--data '{"paths":[
<file_path1>,
<file_path2>,
]}' | jq
```

The document for rest api specification can be found [here](https://cernbox.cern.ch/pdf-viewer/public/vLhBpHDdaXJSqwW/WLCG%20Tape%20REST%20API%20reference%20document.pdf)


### resurrect_dids.py

These are a set of scripts used to resurrect removed dids from the DELETED_DIDS table and then restore them in Rucio.
In order to do that one has to:

1. Ressurect the DIDs 
2. Re-link DIDs (attach blocks to datasets and files to blocks)
3. Make sure that the files actually exist in some RSE (possibly using gfal-stat)
4. Manually add a replica that points to the existing file in the above found RSE

