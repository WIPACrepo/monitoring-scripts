import argparse
import re
import requests
from pprint import pprint


def list_indexes(host):
    ret = requests.get(f'{host}/_cat/indices')
    ret.raise_for_status()
    indexes = []
    for line in ret.text.split('\n'):
        try:
            if line.strip():
                indexes.append(line.split()[2])
        except Exception:
            print('line:',line)
            raise
    return indexes

def delete_index(host, index, dry=False):
    print('deleting index', index)
    if not dry:
        ret = requests.delete(f'{host}/{index}')
        ret.raise_for_status()

def main():
    parser = argparse.ArgumentParser('delete elasticsearch indexes')
    parser.add_argument('--host', default='http://elk-1.icecube.wisc.edu:9200', help='elasticsearch host')
    parser.add_argument('index_pattern', help='index regex pattern')
    parser.add_argument('--dry-run', action='store_true', help='dry run')
    args = parser.parse_args()

    indexes = list_indexes(args.host)
    print(args.index_pattern)
    for index in indexes:
        if re.match(args.index_pattern, index):
            delete_index(args.host, index, dry=args.dry_run)
    


if __name__ == '__main__':
    main()