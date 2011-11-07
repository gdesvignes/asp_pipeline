import sys, os
import database as db
from optparse import OptionParser


full_usage = """
usage : asp_process.py [options] 

  [-h, --help]        : Display this help
  [-v, --verbose]     : Verbose mode
  [-p, --pulsar %s]   : Select pulsar to process

"""
usage = "usage: %prog [options]"

class ASP:

    def __init__(self, database, opts):
        self.database = database
	self.opts = opts
	self.psrname = opts.pulsar

    def connect(self):
        # Connect to DB
	self.db = db.Database(db = self.database)
	self.DBconn = self.db.conn
	self.DBcursor = self.db.cursor

    def close(self):
        self.DBconn.close()

    def exemple_command(self):	
        self.connect()
        query = "select distinct psrname, count(*), mjd, (55685-mjd)/count(*), SUM(t_int)/(count(*)*60.) from asp_headers as H where H.period<0.03 and mode='PSR' and H.nchan>20 and H.psrname = '%s' and H.mode='PSR';"%self.psrname
        self.DBcursor.execute(query)
        result_query = [list(row) for row in self.DBcursor.fetchall()]
        self.close()
	print result_query


def main():
    parser = OptionParser(usage)

    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="Verbose mode")
    parser.add_option("-p", "--pulsar", type="string", dest="pulsar", default=None, help="Select pulsar to process")

    (opts, args) = parser.parse_args()

    if len(sys.argv) < 2:
        print full_usage
        sys.exit(0)

    Asp = ASP("ASP", opts)

    Asp.exemple_command()

if __name__ == '__main__':

    main()


