import requests
import json
from urllib.parse import urlparse
import os


def parse_apihost_path_map(urls):
    parsed_urls = [urlparse(url) for url in urls]

    host_files_map = {}
    for url in parsed_urls:
        hostname = 'https://' + url.netloc
        if hostname not in host_files_map:
            host_files_map[hostname] = []
        host_files_map[hostname].append(url.path)

    api_host_files_map = {}
    for hostname, files in host_files_map.items():
        response = requests.get(hostname + '/.well-known/wlcg-tape-rest-api',
                                verify='/etc/grid-security/certificates',
                                cert=(os.environ['X509_USER_PROXY'],
                                      os.environ['X509_USER_PROXY']),
                                timeout=180
                                )
        tape_host = response.json()["endpoints"][0]["uri"]
        api_host_files_map[tape_host] = files
    return api_host_files_map


def get_locality(urls):
    host_files_map = parse_apihost_path_map(urls)

    for hostname, files in host_files_map.items():
        response = requests.post(hostname + '/archiveinfo',
                                 data=json.dumps({"paths": files}),
                                 verify='/etc/grid-security/certificates',
                                 cert=(os.environ['X509_USER_PROXY'],
                                       os.environ['X509_USER_PROXY']),
                                 headers={'Content-Type': 'application/json'},
                                 timeout=180
                                 )
        print(response.text)


if __name__ == "__main__":
    urls = ["davs://<hostname>:<port><path>"]
    get_locality(urls)
