import os
import shutil
import re

import whoosh.highlight as highlight
import whoosh.index as widx
from whoosh.analysis import CharsetFilter, StemmingAnalyzer
from whoosh.fields import Schema, ID, NUMERIC, TEXT
from whoosh.qparser import QueryParser
from whoosh.support.charset import accent_map

from .TermOutput import msg, wrapWithColor, fg, bg, attr, stylize

class ANSIFormatter(highlight.Formatter):
    wsRe = re.compile(r"\s+")
    def format_token(self, text, token, replace=False):
        tokentext = highlight.get_text(text, token, replace)
        return stylize(tokentext, fg('cyan') + attr('bold'))

    def format(self, fragments, replace=False):
        s = super().format(fragments, replace).strip()
        s = ' '.join(self.wsRe.split(s))
        return wrapWithColor(s, firstIndent=4, indent=4)

class TextSearch:
    @staticmethod
    def init(dataDir):
        pth = os.path.join(dataDir, ".whoosh")
        try:
            os.mkdir(pth)
        except FileExistsError:
            shutil.rmtree(pth)
            os.mkdir(pth)

        analyzer= StemmingAnalyzer() | CharsetFilter(accent_map)
        schema = Schema(md5=ID(stored=True),
                        page=NUMERIC(stored=True),
                        text=TEXT(stored=True, analyzer=analyzer))
        widx.create_in(pth, schema)

    def __init__(self, dataDir):
        self.ix = widx.open_dir(os.path.join(dataDir, ".whoosh"))

    def add(self, md5, pages):
        with self.ix.writer() as writer:
            for i,page in enumerate(pages):
                writer.add_document(md5=md5, page=i, text=page)

    def delete(self, md5):
        with self.ix.writer() as writer:
            writer.delete_by_term("md5", md5)

    def search(self, term, formatter=None):
        if formatter == "html":
            formatter = highlight.HtmlFormatter(tagname='span', classname='match', between='<span class="searchSep"></span>')
        elif formatter == "ansi":
            formatter = ANSIFormatter()
        else:
            formatter = highlight.UppercaseFormatter()

        with self.ix.searcher() as searcher:
            q = QueryParser("text", self.ix.schema).parse(term)
            results = searcher.search(q, limit=None)
            results.fragmenter.charlimit = None
            results.fragmenter.maxchars = 300
            results.fragmenter.surround = 50
            results.formatter = formatter
            pgDict = {}

            for hit in results:
                sk = hit['md5']
                r = dict(page=hit['page'], frag=hit.highlights("text",top=10), score=hit.score)

                if sk in pgDict:
                    pgDict[sk].append(r)
                else:
                    pgDict[sk] = [r]

        hits = []

        for md5, rs in pgDict.items():
            score = sum(x['score'] for x in rs)
            fragments = [dict(page=x['page']+1, frag=x['frag']) for x in sorted(rs, key=lambda x:x['page'])]
            hits.append( (md5, score, fragments) )

        return sorted(hits, key=lambda x:x[1])
