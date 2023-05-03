User data registration


1. Generate user certificate
    
2. Check rucio client is installed

3. Run `export X509_USER_PROXY=$(voms-proxy-info --path)`

4. Update settings and files in `/config/files.json` `/config/options.json`

5. Run `python3 main.py src/config/files.json src/config/options.json --dry-run`

