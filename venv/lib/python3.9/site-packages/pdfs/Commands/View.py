from .Command import Command
from .Completers import citekeyCompleter, attachmentCompleter

class View(Command):
    command = 'view'
    help = "View article PDF and attachements"

    def set_args(self, subparser):
        subparser.add_argument('key', metavar='CITE_KEY', type=str).completer = citekeyCompleter
        subparser.add_argument('label', nargs='?', metavar='NAME', default='PDF', type=str).completer = attachmentCompleter

    def run(self, args):
        import subprocess
        from ..Database import Database
        from ..Exceptions import UserException

        db = Database(dataDir=args.data_dir)

        try:
            e = next(x for x in db.works if x.key() == args.key)
        except StopIteration:
            raise UserException("Key {} not found".format(args.key))

        subprocess.Popen(['xdg-open', db.getFile(e, args.label)])
