import os
import sys
import argparse

import argcomplete

from .Exceptions import AbortException, UserException, WorkExistsException, RepositoryException
from .Database import Database
from .TermOutput import msg
from .Commands import *
from .Commands.Command import Registry
from .Cache import RequestCache

def main():
    parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser(description="Bibliography database manipulation")

    parser.add_argument("--debug", action="store_true")

    parser.add_argument("--logging-level", "-L",
                        help="Logging level: CRITICAL, ERROR, WARNING, INFO, DEBUG",
                        metavar="LEVEL", type=str, default="WARNING")

    parser.add_argument("--data-dir", help="Path to articles directory", type=str, default=None)

    subparsers = parser.add_subparsers(title='Commands', dest='_commandName')

    for cmdType in Registry.commands:
        cmdType().args(subparsers)

    argcomplete.autocomplete(parser)

    args = parser.parse_args()
    if args.debug:
        msg.setup(level="DEBUG")
        try:
            from ipdb import launch_ipdb_on_exception
        except ModuleNotFoundError:
            from contextlib import contextmanager
            def noop():
                yield
            launch_ipdb_on_exception = contextmanager(noop)
    else:
        msg.setup(level=args.logging_level)

    ddir = Database.getDataDir(dataDir=args.data_dir)
    if ddir:
        RequestCache(os.path.join(ddir, ".cache.pkl"))

    try:
        if hasattr(args, "func"):
            if args.debug:
                with launch_ipdb_on_exception():
                    args.func(args)
            else:
                args.func(args)
        else:
            parser.print_usage()
    except UserException as e:
        msg.error("Error: %s", e)
        sys.exit(1)
    except AbortException:
        msg.error("Aborted")
        sys.exit(1)
    except (WorkExistsException, RepositoryException) as e:
        msg.error(str(e))
        sys.exit(1)
    except:
        t,v,_ = sys.exc_info()
        msg.critical("Unhandled exception: %s(%s)", t.__name__, v)
        sys.exit(1)
