from .Command import Command
class WWW(Command):
    command = 'www'
    help = "Spin up http server"

    def set_args(self, subparser):
        subparser.add_argument("--port","-P", help="Port number to listen on", type=int, default=5000)

    def run(self, args):
        import logging
        import mimetypes
        import os

        import flask
        import jinja2

        from ..Database import Database
        from ..HTMLBib import bibContext, authorNorm
        from ..Exceptions import UserException
        from ..Bibtex import unicodeNorm
                
        if not args.debug:
            logging.getLogger('werkzeug').setLevel(logging.ERROR)

        Database(dataDir=args.data_dir)
        flaskApp = flask.Flask("pdfs")
        flaskApp.jinja_env.trim_blocks = True
        flaskApp.jinja_env.lstrip_blocks = True
        flaskApp.jinja_loader=jinja2.PackageLoader("pdfs")

        def mkTagList(db):
            if db.tags:
                return ' '.join('<a class="tags" href="/tag/{0}">{0}</a>'.format(t) for t in sorted(db.tags))

        def keySort(xs):
            return sorted(xs, key=lambda x: x.key())

        def doSearch(tag=None, text=None, author=None, title=None):
            db = Database(dataDir=args.data_dir)

            ctx = dict(article_dir=os.path.basename(os.path.dirname(db.dataDir)), 
                       tags=mkTagList(db))

            if tag:
                ctx['entries'] = bibContext(keySort(filter(lambda x: tag in x.tags, db.works)))
                ctx['search'] = "tag:" + tag
            elif text:
                entries, searchData = [], []
                for result in db.search(text, formatter="html"):
                    entries.append(result['entry'])
                    searchData.append(result)

                bctx = bibContext(entries)

                for c,r in zip(bctx,searchData):
                    c['searchTxt'] = dict(score=r['score'], frags=r['frags'])

                ctx['entries'] = bctx[::-1]
                ctx['search'] = "text:" + text
            elif author:
                def isAuth(e):
                    n, au, ed = set(), e.author(), e.editor()
                    if au:
                        n.update(authorNorm(x.split(', ')[0]) for x in au.split(' and '))
                    if ed:
                        n.update(authorNorm(x.split(', ')[0]) for x in ed.split(' and '))
                    return author in n

                matches = keySort(filter(isAuth, db.works))

                ctx['entries'] = bibContext(matches)
                ctx['search'] = "author:" + author
            elif title:
                def m(x):
                    return title.lower() in unicodeNorm(x.title()).lower()

                ctx['entries'] = bibContext(keySort(filter(m, db.works)))
                ctx['search'] = "title:" + title
            else:
                ctx['entries'] = bibContext(keySort(db.works))

            return ctx

        @flaskApp.route('/')
        def listFiles():
            return flask.render_template('bibliography.html', **doSearch())

        @flaskApp.route('/search')
        def searchFiles():
            query=flask.request.args.get('q', '')
            queryType=flask.request.args.get('t', '')

            if queryType == "text":
                ctx = doSearch(text=query)
            elif queryType == "author":
                ctx = doSearch(author=query)
            elif queryType == "title":
                ctx = doSearch(title=query)
            elif queryType == "tag":
                ctx = doSearch(tag=query)
            else:
                raise RuntimeError("got bad query {}:{}".format(queryType, query))

            return flask.render_template('bibliography.html', **ctx)

        @flaskApp.route('/author/<author>')
        def listFilesByAuthor(author):
            return flask.render_template('bibliography.html', **doSearch(author=author))

        @flaskApp.route('/tag/<tag>')
        def listFilesByTag(tag):
            return flask.render_template('bibliography.html', **doSearch(tag=tag))

        @flaskApp.route('/<key>.pdf')
        def getPdf(key):
            db = Database(dataDir=args.data_dir)
            try:
                pdfFile = next(filter(lambda x: x.key() == key, db.works)).files[0]
            except StopIteration:
                raise KeyError
            resp = flask.make_response(open(os.path.join(db.dataDir, pdfFile), "rb").read())
            resp.content_type = 'application/pdf'
            return resp

        @flaskApp.route('/attachment/<string:key>-<int:idx>.<string:ext>')
        def getAttached(key, idx, ext):
            db = Database(dataDir=args.data_dir)
            try:
                attFile = next(filter(lambda x: x.key() == key, db.works)).files[idx]
            except StopIteration:
                raise KeyError

            filePath = os.path.join(db.dataDir, attFile)
            resp = flask.make_response(open(filePath, "rb").read())
            mime, _ = mimetypes.guess_type(filePath)
            resp.content_type = mime or 'application/octet-stream'
            return resp

        @flaskApp.route('/<key>.bib')
        def getBib(key):
            db = Database(dataDir=args.data_dir)
            e = db.find(key=key)
            resp = flask.make_response(e.bibtex)
            resp.content_type = 'text/plain'
            return resp

        try:
            flaskApp.run(port=args.port)
        except OSError as err:
            if 'Address already in use' in str(err):
                raise UserException("Port {} already in use.".format(args.port))
            else:
                raise
