#!/usr/bin/python3.6

import sys
import csv
from datetime import datetime
import requests
from operator import itemgetter

CITATIONS_FIELDS = ['cites', 'cited']
CITATIONS_QUERY = """
PREFIX cito: <http://purl.org/spar/cito/>
SELECT ?cites ?cited
FROM <https://w3id.org/oc/corpus/br/>
WHERE {
  ?cites cito:cites ?cited .
}
"""

DOI_CITATIONS_FIELDS = ['cites_doi', 'cited_doi']
DOI_CITATIONS_QUERY = """
PREFIX cito: <http://purl.org/spar/cito/>
PREFIX datacite: <http://purl.org/spar/datacite/>
PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
SELECT ?cites_doi ?cited_doi
WHERE {
  ?cites cito:cites ?cited .
  ?cites datacite:hasIdentifier [
    datacite:usesIdentifierScheme datacite:doi ;
    literal:hasLiteralValue ?cites_doi
  ].
  ?cited datacite:hasIdentifier [
    datacite:usesIdentifierScheme datacite:doi ;
    literal:hasLiteralValue ?cited_doi
  ] .
}
"""

IDS_FIELDS = ['identifier', 'identical_val', 'identical_scheme']
IDS_QUERY = """
PREFIX datacite: <http://purl.org/spar/datacite/>
PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
SELECT ?identifier ?identical_val ?identical_scheme
FROM <https://w3id.org/oc/corpus/br/>
FROM <https://w3id.org/oc/corpus/id/>
WHERE {
  ?identifier datacite:hasIdentifier ?identical .
  ?identical literal:hasLiteralValue ?identical_val .
  ?identical datacite:usesIdentifierScheme ?identical_scheme .
}
"""

QUERY_PARAMS = 'LIMIT {limit}\nOFFSET {offset}'

# OpenCitations Public SPARQL API
URL = 'http://opencitations.net/sparql'

# Local blazegraph instance
# HOST = 'http://127.0.0.1:3000'
# URL = f'{HOST}/blazegraph/namespace/kb/sparql'
HEADERS = {'Accept': 'application/sparql-results+json'}


def write_to_csv(fpath, lines, modifier=None):
    with open(fpath, 'a') as fp:
        writer = csv.writer(fp)
        if modifier:
            for row in map(modifier, lines):
                writer.writerow(row)
        else:
            writer.writerows(lines)


def fetch_results(query, fields, limit=10000, offset=0):
    print('.')
    try:
        batch_query = query + QUERY_PARAMS.format(limit=limit, offset=offset)
        res = requests.post(URL, headers=HEADERS, data={'query': batch_query})
        if res.ok:
            data = res.json()
            _getter = itemgetter(*fields)
            return [tuple(k['value'] for k in _getter(d))
                    for d in data['results']['bindings']]
    except Exception as ex:
        print(ex)


def fetch_all(query, fields, modifier=None, batch_size=10000, fpath=None):
    start = datetime.now()
    print(f'Starting to fetch at {start}')
    total = 0
    results = []
    offset = 0
    next_res = fetch_results(query, fields, batch_size, offset=offset)
    while next_res:
        results.extend(next_res)
        offset += batch_size
        next_res = fetch_results(query, fields, batch_size, offset=offset)
        if offset % (batch_size * 10) == 0:
            total += batch_size * 10
            t = datetime.now()
            d = t - start
            print(f'\n[{t}] - {offset}...\t({(d)} - {int(total/d.total_seconds())} items/sec)')
            # Append to file and empty
            if fpath:
                write_to_csv(fpath, results, modifier)
            results = []
    return results


_PREFIX_LEN = len('http://purl.org/spar/datacite/')


def normalize_scheme(t):
    return (t[0], t[1], t[2][_PREFIX_LEN:])


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: blazegraph.py {ids|citations} [OUTFILE]\n')
        print('Example: blazegraph.py ids my-ids.csv\n')
        exit(1)
    query_type = sys.argv[1]
    outfile = sys.argv[2] if len(sys.argv) == 3 else None
    if query_type == 'ids':
        fetch_all(IDS_QUERY, IDS_FIELDS, modifier=normalize_scheme,
                  fpath=(outfile or 'identifiers.csv'))
    if query_type == 'citations':
        fetch_all(CITATIONS_QUERY, CITATIONS_FIELDS,
                  fpath=(outfile or 'citations.csv'))
