from .Command import Command
from .Completers import citekeyCompleter, tagCompleter, authorCompleter

class List(Command):
    command = 'list'
    help = "List all items in database"

    def set_args(self, subparser):
        subparser.add_argument("--title", "-t", metavar='REGEX', type=str, default=None)
        subparser.add_argument("--author", "-a", metavar='REGEX', type=str, default=None).completer = authorCompleter
        subparser.add_argument("--year", "-y", metavar='REGEX', type=str, default=None)
        subparser.add_argument("--tag", "-T", metavar='TAG', type=str, default=None).completer = tagCompleter
        subparser.add_argument("--key", "-k", metavar='REGEX', type=str, default=None).completer = citekeyCompleter

    def run(self, args):
        import re
        from ..Database import Database
        from ..AnsiBib import printBibliography

        db = Database(dataDir=args.data_dir)

        gen = iter(db.works)

        def match(g, f, r):
            if r:
                return filter(lambda x: getattr(x, f)() and re.search(r, getattr(x, f)(), re.I), g)
            return g

        gen = match(gen, 'title', args.title)
        gen = match(gen, 'author', args.author)
        gen = match(gen, 'year', args.year)
        gen = match(gen, 'key', args.key)
        if args.tag:
            gen = filter(lambda x: args.tag in x.tags, gen)


        printBibliography(sorted(gen, key=lambda x: x.key()))
