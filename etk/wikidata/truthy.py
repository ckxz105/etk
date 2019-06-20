from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from rdflib.query import ResultException


class TruthyUpdater:
    def __init__(self, endpoint, dryrun=False, user=None, passwd=None):
        self.endpoint = SPARQLUpdateStore(endpoint,
                                          default_query_method='POST')
        if user:
            self.endpoint.setCredentials(user, passwd)
        self.dryrun = dryrun

    def build_truthy(self, np_list):
        self.insert_truthy_rank(np_list)
        self.delete_normal_rank(np_list)

    def update(self, query):
        if self.dryrun:
            action = 'INSERT' if 'INSERT' in query else 'DELETE'
            query = query.replace(action, 'CONSTRUCT')
            # print(query)
            try:
                res = self.endpoint.query(query)
                print('### About to {} following triples:'.format(action))
                for row in res:
                    print(' '.join(e.n3() for e in row))
            except ResultException:
                pass
        else:
            self.endpoint.update(query)

    def insert_truthy_rank(self, np_list):
        values = ' '.join('( wd:%(node)s p:%(p)s ps:%(p)s psn:%(p)s wdt:%(p)s wdtn:%(p)s )' % {'node': n, 'p': p} for n, p in np_list)
        query = '''
            prefix wdt: <http://www.wikidata.org/prop/direct/>
            prefix wdtn: <http://www.wikidata.org/prop/direct-normalized/>
            prefix wdno: <http://www.wikidata.org/prop/novalue/>
            prefix wds: <http://www.wikidata.org/entity/statement/>
            prefix wdv: <http://www.wikidata.org/value/>
            prefix wdref: <http://www.wikidata.org/reference/>
            prefix wd: <http://www.wikidata.org/entity/>
            prefix wikibase: <http://wikiba.se/ontology#>
            prefix p: <http://www.wikidata.org/prop/>
            prefix pqv: <http://www.wikidata.org/prop/qualifier/value/>
            prefix pq: <http://www.wikidata.org/prop/qualifier/>
            prefix ps: <http://www.wikidata.org/prop/statement/>
            prefix psn: <http://www.wikidata.org/prop/statement/value-normalized/>
            prefix prv: <http://www.wikidata.org/prop/reference/value/>
            prefix psv: <http://www.wikidata.org/prop/statement/value/>
            prefix prn: <http://www.wikidata.org/prop/reference/value-normalized/>
            prefix pr: <http://www.wikidata.org/prop/reference/>
            prefix pqn: <http://www.wikidata.org/prop/qualifier/value-normalized/>
            prefix skos: <http://www.w3.org/2004/02/skos/core#>
            prefix prov: <http://www.w3.org/ns/prov#>
            prefix schema: <http://schema.org/'>
            prefix bd: <http://www.bigdata.com/rdf#>
            prefix bds: <http://www.bigdata.com/rdf/search#>
        INSERT {
          ?statement a wikibase:BestRank .
          ?node ?wdt ?psv ;
                ?wdtn ?psnv .
        } WHERE {
          %s
            {
                ?node ?p ?statement .
                ?statement wikibase:rank wikibase:PreferredRank ;
                           ?ps ?psv .
                OPTIONAL { ?statement ?psn ?psnv }
                FILTER NOT EXISTS { ?statement a wikibase:BestRank }
            }
            UNION
            {
              ?node ?p ?statement .
              ?statement wikibase:rank wikibase:NormalRank ;
                         ?ps ?psv .
              OPTIONAL { ?statement ?psn ?psnv }
              FILTER NOT EXISTS { ?statement a wikibase:BestRank }
              FILTER NOT EXISTS { ?node ?p [ wikibase:rank wikibase:PreferredRank ] }
            }
        }
        ''' % ('VALUES (?node ?p ?ps ?psn ?wdt ?wdtn ) { %s }' % values)
        self.update(query)

    def delete_normal_rank(self, np_list):
        values = ' '.join('( wd:%(node)s p:%(p)s wdt:%(p)s wdtn:%(p)s )' % {'node': n, 'p': p} for n, p in np_list)
        query = '''
            prefix wdt: <http://www.wikidata.org/prop/direct/>
            prefix wdtn: <http://www.wikidata.org/prop/direct-normalized/>
            prefix wdno: <http://www.wikidata.org/prop/novalue/>
            prefix wds: <http://www.wikidata.org/entity/statement/>
            prefix wdv: <http://www.wikidata.org/value/>
            prefix wdref: <http://www.wikidata.org/reference/>
            prefix wd: <http://www.wikidata.org/entity/>
            prefix wikibase: <http://wikiba.se/ontology#>
            prefix p: <http://www.wikidata.org/prop/>
            prefix pqv: <http://www.wikidata.org/prop/qualifier/value/>
            prefix pq: <http://www.wikidata.org/prop/qualifier/>
            prefix ps: <http://www.wikidata.org/prop/statement/>
            prefix psn: <http://www.wikidata.org/prop/statement/value-normalized/>
            prefix prv: <http://www.wikidata.org/prop/reference/value/>
            prefix psv: <http://www.wikidata.org/prop/statement/value/>
            prefix prn: <http://www.wikidata.org/prop/reference/value-normalized/>
            prefix pr: <http://www.wikidata.org/prop/reference/>
            prefix pqn: <http://www.wikidata.org/prop/qualifier/value-normalized/>
            prefix skos: <http://www.w3.org/2004/02/skos/core#>
            prefix prov: <http://www.w3.org/ns/prov#>
            prefix schema: <http://schema.org/'>
            prefix bd: <http://www.bigdata.com/rdf#>
            prefix bds: <http://www.bigdata.com/rdf/search#>
          DELETE {
            ?statement a wikibase:BestRank .
            ?node ?wdt ?value ;
              ?wdtn ?no .
          } WHERE {
            %s
            ?node ?p ?statement ;
                  ?p [ wikibase:rank wikibase:PreferredRank ] .
            ?statement a wikibase:BestRank ;
                       wikibase:rank wikibase:NormalRank .
          }
        ''' % ('VALUES (?node ?p ?wdt ?wdtn ) { %s }' % values)
        self.update(query)
