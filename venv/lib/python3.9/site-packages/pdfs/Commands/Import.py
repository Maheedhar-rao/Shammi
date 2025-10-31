from .Command import Command
from argcomplete.completers import FilesCompleter
from .Completers import citekeyCompleter

class Import(Command):
    command = 'import'
    help = "Import entries from other database"

    def set_args(self, subparser):
        subparser.add_argument('src', metavar='SRC_REPO', type=str, help='Path to "articles" directory').completer = FilesCompleter
        subparser.add_argument("keys", help="Keys to import", nargs='+', type=str).completer = citekeyCompleter

    def run(self, args):
        from ..Database import Database
        from ..AnsiBib import printWork

        dbDest = Database(dataDir=args.data_dir)
        dbSrc = Database(dataDir=args.src)

        for k in args.keys:
            e = dbDest.copyFromDb(dbSrc, k)
            printWork(e)
