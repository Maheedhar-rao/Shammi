from .Command import Command
from argcomplete.completers import FilesCompleter

class Aux2Bib(Command):
    command = 'aux2bib'
    help = "Read LaTeX .aux file and dump a .bib file contining all used citations"

    def set_args(self, subparser):
        subparser.add_argument('aux', metavar='AUX_FILE', type=str).completer = FilesCompleter("aux", directories=False)
        subparser.add_argument('bib', metavar='BIB_FILE', type=str).completer = FilesCompleter("bib", directories=False)

    def run(self, args):
        import re
        from ..Database import Database
        from ..TermOutput import msg

        db = Database(dataDir=args.data_dir)
        auxCiteArgs = re.findall(r"\\(?:bibcite|citation)\{([^}]+)\}", open(args.aux, "r").read())
        auxKeys = sorted(set(x.strip() for y in auxCiteArgs for x in y.split(',')))

        btexs, missing = [], []

        for key in auxKeys:
            try:
                btexs.append(next(x.bibtex for x in db.works if x.key() == key))
            except StopIteration:
                missing.append(key)

        if btexs:
            with open(args.bib, "w") as bib:
                bib.write('\n'.join(btexs))

        if missing:
            msg.warning("Citation keys missed: %s", str(missing))
