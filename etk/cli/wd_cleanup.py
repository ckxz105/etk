from etk.wikidata.truthy import TruthyUpdater


def add_arguments(parser):
    parser.add_argument('-e', '--endpoint', action='store', type=str, dest='endpoint')
    parser.add_argument('-c', '--creator', default='http://www.isi.edu/datamart', action='store', type=str,
                        dest='creator')
    parser.add_argument('--user', action='store', default=None, type=str, dest='user')
    parser.add_argument('--passwd', action='store', default=None, type=str, dest='passwd')
    parser.add_argument('--dry-run', action='store_true', default=False, dest='dryrun')


def run(args):
    print('Start deleting statements created by {} in endpoint {}'.format(args.creator, args.endpoint))
    if args.dryrun:
        print('Mode: Dryrun')
    tu = TruthyUpdater(args.endpoint, args.dryrun, args.user, args.passwd)
    creator = args.creator

    # delete all wdt:CXXX
    values = ['(wdt:C{})'.format(i) for i in range(3001, 3020)]

    query = '''
    DELETE {
        ?s ?p ?o
    }
    WHERE {
        VALUES (?p) {
            %s
        }
        ?s ?p ?o
    }''' % (' '.join(values))
    try:
        # print(query)
        tu.update(query)
    except:
        pass

    # delete all wdtn:CXXX
    values = ['(wdtn:C{})'.format(i) for i in range(3001, 3020)]

    query = '''
    DELETE {
        ?s ?p ?o
    }
    WHERE {
        VALUES (?p) {
            %s
        }
        ?s ?p ?o
    }''' % (' '.join(values))
    try:
        # print(query)
        tu.update(query)
    except:
        pass

    print('Deletion complete!')
