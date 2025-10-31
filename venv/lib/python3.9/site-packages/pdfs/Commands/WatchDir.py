from .Command import Command
from ..Exceptions import UserException
from argcomplete.completers import DirectoriesCompleter

class WatchDir(Command):
    command = 'watch'
    help = "Watch a directory for new pdf files to add"

    def set_args(self, subparser):
        subparser.add_argument('dir', metavar='DIRECTORY', type=str).completer = DirectoriesCompleter

    def run(self, args):
        import os.path
        import inotify.adapters
        from pdfminer.pdfparser import PDFSyntaxError
        from ..Database import Database
        from ..TermOutput import msg
        from ..ExtractDoi import entryFromUser, entryFromPdf
        from ..Exceptions import WorkExistsException, AbortException
        from ..AnsiBib import printWork

        wd = os.path.abspath(args.dir)
        if not os.path.isdir(wd):
            raise UserException("%s is not a directory" % wd)

        inot = inotify.adapters.Inotify()
        inot.add_watch(wd.encode())
        msg.info("Watching %s for new pdf files...", wd)

        try:
            createdFiles = set()
            for event in inot.event_gen():
                if event is not None:
                    _, type_names, _, filename = event
                    msg.debug("INOTIFY: %s (%s)", filename.decode(), ', '.join(type_names))
                    if 'IN_CREATE' in type_names:
                        f = filename.decode("utf-8")
                        if f.lower().endswith(".pdf") and f[0] != '.':
                            createdFiles.add(filename)
                    elif 'IN_CLOSE_WRITE' in type_names and filename in createdFiles:
                        createdFiles.remove(filename)
                        try:
                            newFile = filename.decode("utf-8")
                            newFilePath = os.path.join(wd, newFile)
                            db = Database(dataDir=args.data_dir)
                            e = db.find(pdfFname=newFilePath)
                            if e:
                                raise WorkExistsException("new file {} exists in database as {}".format(newFile, e.key()))
                            else:
                                msg.info("new file: %s", newFile)
                                entry = entryFromPdf(newFilePath) or entryFromUser(newFilePath)
                                db.add(entry, newFilePath, [], [])
                                printWork(entry)
                        except WorkExistsException as e:
                            msg.warning(str(e))
                        except AbortException:
                            msg.warning("PDF import aborted")
                        except PDFSyntaxError:
                            msg.warning("file %s seems broken", newFile)
        except KeyboardInterrupt:
            pass
        finally:
            inot.remove_watch(wd)
