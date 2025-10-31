from .Command import Command

class Search(Command):
    command = 'search'
    help = "Search full text of PDF"

    def set_args(self, subparser):
        subparser.add_argument("query", nargs="+", type=str)

    def run(self, args):
        from ..AnsiBib import printWork
        from ..TermOutput import msg, fg, attr, stylize, printRule
        from ..Database import Database

        db = Database(dataDir=args.data_dir)

        results = db.search(' '.join(args.query), formatter="ansi")
        for i,result in enumerate(results):
            printWork(result['entry'])
            msg("Score: " + stylize("{: 4.3f}".format(result['score']), fg("yellow"), attr('bold')))
            msg()
            for frag in result['frags']:
                printRule("page {:4d}".format(frag['page']), width=50, color=fg("blue")+attr('bold'))
                msg(frag['frag'])

            if i < len(results)-1:
                printRule()
