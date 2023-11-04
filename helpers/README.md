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
6. For a Read Test `python3 test_http_tape_rest_api.py --rse RSENAME --mode read --ruleid RULEID`
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