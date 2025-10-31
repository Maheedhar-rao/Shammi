from .Command import Command
from .Completers import citekeyCompleter

class Bibtex(Command):
    command = 'bibtex'
    help = "Dump bibtex for keys"

    def set_args(self, subparser):
        subparser.add_argument('keys', metavar='CITE_KEY', nargs="*", type=str).completer = citekeyCompleter
        subparser.add_argument('--all', '-a', action='store_true')

    def run(self, args):
        from ..Database import Database
        from ..TermOutput import msg

        db = Database(dataDir=args.data_dir)

        btexs = []
        missing = []

        if args.keys and args.all:
            msg.error("Specification of keys and the --all option are mutually exclusive")
        elif args.all:
            btexs = [x.bibtex for x in sorted(db.works, key=lambda x: x.key())]
        elif args.keys:
            for key in args.keys:
                try:
                    btexs.append(next(x.bibtex for x in db.works if x.key() == key))
                except StopIteration:
                    missing.append(key)
        else:
            msg.error("Must specify keys to list or --all")

        if btexs:
            print('\n'.join(btexs))
        if missing:
            msg.warning("Could not find keys: %s", str(missing))
