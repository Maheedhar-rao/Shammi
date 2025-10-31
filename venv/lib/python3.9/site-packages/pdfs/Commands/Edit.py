import os
import tempfile

from .Command import Command

class Edit(Command):
    command = 'edit'
    help = "Edit bibtex, metadata and file attachments"

    def set_args(self, subparser):
        subparser.add_argument('key', metavar='CITE_KEY', type=str)
        subparser.add_argument('--edit-bibtex', '-B',  action="store_true")
        subparser.add_argument('--add-tags', '-a',  nargs='+', default=[], metavar='TAG', type=str)
        subparser.add_argument('--del-tags', '-d',  nargs='+', default=[], metavar='TAG', type=str)
        subparser.add_argument('--add-attachments', '-A',  nargs='+', default=[], metavar='ATTACHMENT', type=str)
        subparser.add_argument('--del-attachments', '-D',  nargs='+', default=[], metavar='NAME', type=str)
        subparser.add_argument('--delete-entry',  action="store_true")

    def run(self, args):
        import re
        from ..TermOutput import msg
        from ..Database import Database
        from ..Exceptions import UserException
        from ..Bibtex import bibtexTypes

        _keyRe = re.compile(r"\s*@\s*(?:"+'|'.join(bibtexTypes)+r")\s*\{\s*([^\s,]+)\s*,")

        db = Database(dataDir=args.data_dir)

        try:
            e = next(x for x in db.works if x.key() == args.key)
        except StopIteration:
            raise UserException("Key {} not found".format(args.key))

        if args.add_tags or args.del_tags:
            e.tags = sorted((set(e.tags) | set(args.add_tags)) - set(args.del_tags))
            db.save()

        if args.del_attachments:
            for n in args.del_attachments:
                db.removeAttachment(args.key, n)

        if args.add_attachments:
            for n in args.add_attachments:
                db.attach(args.key, n)

        if args.edit_bibtex:
            oldBib = e.bibtex
            newBib = spawnEditor(oldBib, 'bib')
            if oldBib != newBib:
                oldKey = e.key()
                newKey = _keyRe.findall(newBib)
                if len(newKey) != 1:
                    tmpf,tmpfname = tempfile.mkstemp(dir="/tmp", prefix="edit-of-{}.".format(oldKey), suffix=".bib", text=True)
                    os.write(tmpf, newBib.encode())
                    os.close(tmpf)
                    raise UserException("Failed to parse new bibtex entry. Work in progress written to {}".format(tmpfname))
                newKey = newKey[0]

                if newKey != oldKey:
                    msg.info("%s → %s", oldKey, newKey)
                    e.set_key(newKey)

                e.bibtex = newBib
            db.save()

        if args.delete_entry:
            db.delete(args.key)


def spawnEditor(text, filetype='txt'):
    tmpf,tmpfname = tempfile.mkstemp(suffix="."+filetype, text=True)
    os.write(tmpf, text.encode())
    os.close(tmpf)
    editor=os.environ.get("VISUAL") or "vi"
    os.system("{} {}".format(editor, tmpfname))
    newText = open(tmpfname).read()
    os.unlink(tmpfname)
    return newText
