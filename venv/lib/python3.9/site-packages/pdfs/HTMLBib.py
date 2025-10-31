from .BibFormatter import BibFormatter
from .Bibtex import unicodeNorm, JournalAbbr

def authorNorm(x):
    return unicodeNorm(x).lower().replace(' ', '_')

class CtxBib(BibFormatter):
    def title(self):
        t = super().title()
        return '<span class="title">' + t + '</span>'

    def year(self):
        y = super().year()
        return "<b>" + y + "</b>"

    def booktitle(self):
        t = super().booktitle()
        if t:
            return "<i>" + t + "</i>"

    def volume(self):
        v = super().volume()
        if v:
            return "<b>" + v + "</b>"

    def journal(self):
        j = super().journal()
        if j:
            ab = JournalAbbr()
            return "<i>" + ab(j) + "</i>"

    def doi(self):
        d = super().doi()
        return '<a class="doi" target="_blank" href="https://dx.doi.org/{d}">doi:{d}</a>'.format(d=d)

    def person_fmt(self, s):
        ns = []
        last = ' '.join(s.split()[:-1])
        ns.append('<a class="author" href="/author/{}">{}</a>'.format(authorNorm(last), s))
        return ' '.join(ns)

    def tags_fmt(self):
        return self.entry.tags or []

    def attachment(self, a):
        idx = self.entry.fileLabels.index(a)
        key = self.entry.key()

        ext = self.entry.files[idx].split('.')
        if len(ext)>1:
            ext = ext[-1]
        else:
            ext = 'dat'
         
        fname = "{}-{:04d}.{}".format(key, idx, ext)

        return dict(name=a, file=fname)

    def attachments_fmt(self):
        return [self.attachment(a) for a in self.entry.fileLabels[1:]]

    def fmt(self):
        return dict(key=super().key(),
                    reference=super().bib_fmt(),
                    importDate=self.timestamp(),
                    tags=self.tags_fmt(),
                    attachments=self.attachments_fmt())

def bibContext(works):
    return [CtxBib(w).fmt() for w in works]
