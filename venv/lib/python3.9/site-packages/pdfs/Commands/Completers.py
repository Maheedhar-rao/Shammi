import sys
from argcomplete import warn
from ..Database import Database
from ..Exceptions import RepositoryException

def completerWrapper(fn):
    def wrapped(**kwargs):
        completions = []
        try:
            completions = fn(**kwargs)
        except RepositoryException:
            warn("No repository at/below CWD")
        except:
            t,v,_ = sys.exc_info()
            warn("Unhandled exception: %s(%s)" % (t.__name__, v))
        return completions
    return wrapped

@completerWrapper
def citekeyCompleter(**kwargs):
    args, prefix = kwargs['parsed_args'], kwargs['prefix']

    if args._commandName == 'import':
        dd = args.src
    else:
        dd = args.data_dir
    return  [x for x in Database(dataDir=dd).citeKeys if x.startswith(prefix)]

@completerWrapper
def tagCompleter(**kwargs):
    args, prefix = kwargs['parsed_args'], kwargs['prefix']
    return [x for x in Database(dataDir=args.data_dir).tags if x.startswith(prefix)]

@completerWrapper
def authorCompleter(**kwargs):
    args, prefix = kwargs['parsed_args'], kwargs['prefix']
    return [x for x in Database(dataDir=args.data_dir).authors if x.lower().startswith(prefix.lower())]

@completerWrapper
def attachmentCompleter(**kwargs):
    args, prefix = kwargs['parsed_args'], kwargs['prefix']
    e = Database(dataDir=args.data_dir).find(key=args.key)
    if not e:
        return []
    return [x for x in e.fileLabels if x.lower().startswith(prefix.lower())]
