import json
import shutil
import os
import os.path
import re
import time
import unicodedata

from .TermOutput import msg
from .Exceptions import WorkExistsException, UserException, RepositoryException
from .BaseWork import Work
from .WorkTypes import *
from .TextSearch import TextSearch
from .ReadPdf import getPdfTxt, md5sum
from .HTMLBib import authorNorm

def _nameDbFile(entry, fname, idx):
    author = entry.author() or entry.editor()

    author = author.split(',')[0]
    title = entry.title() or entry.booktitle()
    year = str(entry.year())
    dbFname = "{}-{}-{}".format(author,year,title.replace(' ', '-'))
    dbFname = unicodedata.normalize("NFKD", dbFname).encode('ascii', 'ignore').decode()
    dbFname = re.sub(r'[^a-zA-Z0-9\-]', '', dbFname)
    if len(dbFname) > 192:
        dbFname = dbFname[:192]

    if idx > 0:
        dbFname = "{}_SI{:04d}".format(dbFname, idx)

    spl = os.path.basename(fname).split('.')
    if len(spl)>1:
        ext = spl[-1]
        return "{}.{}".format(dbFname, ext)
    return dbFname


class FileLock:
    def __init__(self, lockFname):
        self.lockFname = lockFname
    def __enter__(self):
        if os.path.exists(self.lockFname):
            otherPid = int(open(self.lockFname).read())
            msg.warning("Waiting for lock on document repository (pid %d has it)", otherPid)
            while os.path.exists(self.lockFname):
                time.sleep(0.2)
        with open(self.lockFname, "w") as f:
            print(os.getpid(), file=f)
    def __exit__(self, *args):
        os.unlink(self.lockFname)


class Database:
    @classmethod
    def init(cls, dataDir=None, clobber=False):
        dataDir = dataDir or os.path.join(os.path.abspath('.'), 'articles')
        try:
            os.mkdir(dataDir)
        except FileExistsError:
            if clobber:
                shutil.rmtree(dataDir)
                os.mkdir(dataDir)
                msg.warning("Clobbered %s", dataDir)
            else:
                raise FileExistsError("Document repository already exists at "+dataDir)
        json.dump([], open(os.path.join(dataDir, ".metadata.json"), "w"))
        TextSearch.init(dataDir)
        return cls(dataDir=dataDir)
            
    def find(self, *, pdfFname=None, key=None):
        if pdfFname:
            pdfMd5 = md5sum(pdfFname)
            try:
                return next(x for x in self.works if pdfMd5 in x.md5s)
            except StopIteration:
                return
        if key:
            try:
                return next(x for x in self.works if key == x.key())
            except StopIteration:
                return

    @staticmethod
    def getDataDir(*, dataDir=None):
        if not dataDir:
            lastDir, root = '', os.path.abspath('.')
            while lastDir != root:
                if os.path.exists(os.path.join(root, "articles/.metadata.json")):
                    dataDir = os.path.join(root, "articles")
                    break
                else:
                    lastDir = root
                    root = os.path.abspath(os.path.join(root, ".."))
        return dataDir

    def __init__(self, *, dataDir=None):
        self.works = None
        dataDir = self.getDataDir(dataDir=dataDir)
        if not dataDir:
            raise RepositoryException("Could not find a document repository here (or any parent up to /)")
        self.dataDir = dataDir
        self.metaFile= os.path.join(dataDir, ".metadata.json")
        self.metaLockFile = os.path.join(dataDir, ".metadata.lck")
        with FileLock(self.metaLockFile):
            self.works = [Work.from_db(**d) for d in json.load(open(os.path.join(dataDir, ".metadata.json")))]

        self.textSearch = TextSearch(self.dataDir)

        self.tags = set()
        for w in self.works:
            self.tags.update(w.tags)

    def save(self):
        with FileLock(self.metaLockFile):
            json.dump([x.toDict() for x in self.works], open(os.path.join(self.dataDir, ".metadata.json"), "w"), indent=1, sort_keys=True)

    def attach(self, key, filename):
        e = self.find(key=key)
        if not e:
            raise UserException("Key {} not found.".format(key))

        name = os.path.basename(filename)

        md5 = md5sum(filename)
        if md5 in e.md5s:
            fname = e.fileLabels[e.md5s.index(md5)]
            raise UserException("File exists as {} under key {}.".format(fname, key))

        siFileName =  _nameDbFile(e, filename, len(e.fileLabels))

        shutil.copyfile(filename, os.path.join(self.dataDir, siFileName))

        e.md5s.append(md5)
        e.fileLabels.append(name)
        e.files.append(siFileName)
        self.save()

    def removeAttachment(self, key, name):
        e = self.find(key=key)
        if not e:
            raise UserException("Key {} not found.".format(key))

        try:
            i = e.fileLabels.index(name)
        except ValueError:
            raise UserException("Attachment {} not found.".format(name))

        os.unlink(os.path.join(self.dataDir, e.fileLabels[i]))
        del e.md5s[i]
        del e.fileLabels[i]
        del e.files[i]
        self.save()

    def add(self, entry, pdfFname, suppFnames, tags):
        # Check for existing pdf
        pdfMd5 = md5sum(pdfFname)
        otherWork = self.find(pdfFname=pdfFname)
        if otherWork:
            raise WorkExistsException("{} already exists in database with key {}".format(os.path.basename(pdfFname), otherWork.key()))

        # copy files
        newPdfFname =  _nameDbFile(entry, pdfFname, 0)
        shutil.copyfile(pdfFname, os.path.join(self.dataDir, newPdfFname))
        entry.files.append(newPdfFname)
        entry.md5s.append(pdfMd5)
        entry.fileLabels.append('PDF')

        self.textSearch.add(pdfMd5, getPdfTxt(pdfFname))

        for i,fn in enumerate(suppFnames):
            md5 = md5sum(fn)
            newSiFname =  _nameDbFile(entry, fn, i+1)
            shutil.copyfile(fn, os.path.join(self.dataDir, newSiFname))
            entry.files.append(newSiFname)
            entry.md5s.append(md5)
            entry.fileLabels.append(os.path.basename(fn))

        # ensure unique cite key 
        citeKeys = self.citeKeys
        oldKey = entry.key()
        newKey = oldKey
        suffix = 'a'

        while newKey in citeKeys:
            newKey = oldKey + suffix
            suffix = chr(ord(suffix)+1)

        entry.set_key(newKey)

        for t in tags:
            entry.tags.append(t)
        self.tags.update(tags)

        self.works.append(entry)
        self.save()

    def getFile(self, entryThing, name='PDF'):
        if isinstance(entryThing, str):
            entry = self.find(key=entryThing)
        else:
            entry = entryThing
        i = entry.fileLabels.index(name)
        fname = entry.files[i]
        return os.path.join(self.dataDir, fname)

    def delete(self, key):
        oldEntry = self.find(key=key)
        newWorks = [w for w in self.works if w is not oldEntry]
        if len(newWorks) == len(self.works):
            raise UserException("Key {} not in repository".format(key))

        self.textSearch.delete(oldEntry.md5s[0])
        
        self.works = newWorks
        self.save()

    def copyFromDb(self, dbSrc, key):
        eSrc = dbSrc.find(key=key)
        eDst = Work.from_db(**eSrc.toDict())

        for lbl in eDst.fileLabels:
            srcFile = dbSrc.getFile(eSrc, lbl)
            dstFile = self.getFile(eDst, lbl)
            shutil.copyfile(srcFile, dstFile)

        self.textSearch.add(eDst.md5s[0], getPdfTxt(self.getFile(eDst, "PDF")))

        self.works.append(eDst)
        self.tags.update(eDst.tags)
        self.save()
        return eDst

    def search(self, query, formatter=None):
        results = []
        for md5, score, frags in self.textSearch.search(query, formatter):
            entry = next(filter(lambda x, md5=md5: x.md5s[0] == md5, self.works))
            results.append( dict(entry=entry, score=score, frags=frags) )
        return results

    @property
    def citeKeys(self):
        return [x.key() for x in self.works]


    @property
    def authors(self):
        n = set()
        for e in self.works:
            au, ed = e.author(), e.editor()
            if au:
                n.update(authorNorm(x.split(', ')[0]) for x in au.split(' and '))
            if ed:
                n.update(authorNorm(x.split(', ')[0]) for x in ed.split(' and '))
        return list(n)
