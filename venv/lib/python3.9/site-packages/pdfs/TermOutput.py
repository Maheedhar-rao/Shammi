from .Bases import Singleton
from colored import stylize, fg, bg, attr
import sys
import shutil
import re

_ansiRe = re.compile(r"((?:\x1b\[[0-9;]+?m)+)")
_wsRe = re.compile(r"(\s+)")

def printRule(label='', *, ch='-', width=None, color=None):
    if not color:
        color=fg("white") +  attr('dim')
    if not width:
        width = max(1, shutil.get_terminal_size()[0]-4)
    if label:
        w = width - len(label) - 2
        lwidth = w//2 
        rwidth = w-lwidth
        lwidth, rwidth = max(0,lwidth), max(0,rwidth)
        print(stylize(ch*lwidth + " " + label + " " + ch*rwidth, color))
    else:
        print(stylize(ch*width, color))

def wrapWithColor(s, width=None, firstIndent=0,indent=3):
    if not width:
        width = min(100, shutil.get_terminal_size()[0])

    splt =  sum([[y for y in _ansiRe.split(x)] for x in _wsRe.split(s)], [])
    splt = [x for x in splt if x]

    lines = []
    lastAnsi = '\x1b[0m'
    breakIdx = 0

    words = [" "*firstIndent, lastAnsi]

    for frag in splt:
        if frag[0] == '\x1b':
            lastAnsi = frag
        elif frag[0] == ' ':
            breakIdx = len(words)
        words.append(frag)

        lineSize = sum(len(w) for w in words[:-1] if w and w[0] != '\x1b')
        if frag.strip():
            lineSize += len(frag)

        if lineSize > width:
            lines.append("".join(words[:breakIdx]).rstrip())
            words = [" "*indent, lastAnsi] + [x for x in words[breakIdx:] if x.strip()]

    lastLine = "".join(words).rstrip(' ')
    if lastLine:
        lines.append(lastLine)

    return "\n".join(lines)

class TermOutput(metaclass=Singleton):
    @property
    def width(self):
        return shutil.get_terminal_size()[0]
    def __init__(self):
        self.setup()

    def setup(self, *,level='info', term=None):
        l = level.lower()
        if l == 'debug':
            self.logLevel = 4
        elif l == 'info':
            self.logLevel = 3
        elif l == 'warning':
            self.logLevel = 2
        elif l == 'error':
            self.logLevel = 1
        elif l == 'critical':
            self.logLevel = 0
        else:
            raise RuntimeError("Invalid log level")
        if term is not None:
            self.isTermE = term
            self.isTermO = term
        else:
            self.isTermE = sys.stderr.isatty()
            self.isTermO = sys.stdout.isatty()

        if self.isTermE:
            self.icos = [stylize("*", fg("white") + bg("red") + attr('bold')),
                         stylize("*", fg("red") + attr('bold')),
                         stylize("*", fg("yellow") + attr('bold')),
                         stylize("*", fg("cyan")),
                         stylize("*", fg("white") + attr('dim'))]
        else:
            self.icos = ["CRIT", "ERR ", "WARN", "INFO", "DBUG"]

    def _plog(self, lvl, msg, args):
        if self.logLevel >= lvl:
            print(self.icos[lvl], msg % args, file=sys.stderr)

    def debug(self, msg, *args): 
        self._plog(4, msg, args)

    def info(self, msg, *args): 
        self._plog(3, msg, args)

    def warning(self, msg, *args): 
        self._plog(2, msg, args)

    def error(self, msg, *args): 
        self._plog(1, msg, args)

    def critical(self, msg, *args): 
        self._plog(0, msg, args)

    def __call__(self, fmt='', *args):
        if args:
            s = fmt % args
            if not self.isTermO:
                s = _ansiRe.sub('', s)
        else:
            s=fmt
        print(s)

msg = TermOutput()
