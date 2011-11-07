#!/usr/bin/env python
import sys
import warnings
import pyodbc
#import prettytable

if sys.platform.startswith("win"):
    # Connecting from Windows
    DATABASES = {
        'common': {
            'DATABASE': 'palfa-common',
            'UID': 'sequenceRafal',
            'PWD': 'seq842ce!!',
            'SERVER': 'arecibosql.tc.cornell.edu',
            'DRIVER': '{SQL Native Client}'
        }
    }
elif sys.platform.startswith("linux"):
    # Connecting from Linux
    DATABASES = {
        'common': {
            'DATABASE': 'palfa-common',
            'UID':  'mcgill',
            'PWD':  'pw4sd2mcgill!',
            'HOST': 'arecibosql.tc.cornell.edu',
            'DSN':  'FreeTDSDSN'
            },
        'tracking': {
            'DATABASE': 'palfatracking',
            'UID':  'mcgill',
            'PWD':  'pw4sd2mcgill!',
            'HOST': 'arecibosql.tc.cornell.edu',
            'DSN':  'FreeTDSDSN'
            },
        'local-PALFA': {
            'DATABASE': 'PALFA', # DB name is case sensitive
            'UID':  'desvignes',
            'PWD':  '0244bon',
            'HOST': 'gemini',
            'DSN':  'MySQLDSN'
            },
        'local-NBPP': {
            'DATABASE': 'NBPP', # DB name is case sensitive
            'UID':  'desvignes',
            'PWD':  '0244bon',
            'HOST': 'gemini',
            'DSN':  'MySQLDSN'
            },
        'ASP': {
            'DATABASE': 'ASP', # DB name is case sensitive
            'UID':  'desvignes',
            'PWD':  '0244bon',
            'HOST': 'localhost',
            'DSN':  'MySQLDSN'
            }
    }

DEFAULTDB = 'ASP'
DATABASES['default'] = DATABASES[DEFAULTDB]

class Database:
    """Database object for connecting to databases using pyodbc.
    """
    def __init__(self, db="default"):
        """Constructor for Database object.
            
            Input:
                'db': database to connect to. (Default: 'default')
                        (gets passed to 'self.connect')
        """
        self.db = db
        self.connect(db)
    
    def connect(self, db):
        """Establish a database connection. Set self.conn and 
self.cursor.
            
            Input:
                'db': databse to connect to. Must be a key in module's
                        DATABASES dict. (Default: 'default')
            Output:
                None
        """
        if db not in DATABASES:
            warnings.warn("Database (%s) not recognized. Using default (%s)." % (db, DEFAULTDB))
            db = 'default'
	#print 'db', db    
        self.conn = pyodbc.connect(**DATABASES[db])    
        self.cursor = self.conn.cursor()

    def commit(self):
        self.conn.commit()
  
    def close(self):
        """Close database connection.
        """
        self.conn.close()
    """
    def showall(self):
        desc = self.cursor.description
        if desc is not None:
            fields = [d[0] for d in desc] 
            table = prettytable.PrettyTable(fields)
            for row in self.cursor:
                table.add_row(row)
        table.printt()
    """

    def insert(self, query):
        self.cursor.execute(query)
        self.commit()
    
    def findFirst(self, query, dict_result = True):
        self.cursor.execute(query)    
        row = self.cursor.fetchone()
        if dict_result:
            names = [desc[0] for desc in self.cursor.description] 
            dict_rows = dict()
            if row:
                i = 0        
                for name in names:  
                    dict_rows[name] = row[i]
                    i = i + 1
        else:
           dict_rows = row 
            
        return dict_rows

    def findAll(self, query, dict_result = True):
        self.cursor.execute(query)        
        rows = self.cursor.fetchall()
        if dict_result:
            names = [desc[0] for desc in self.cursor.description] # cursor.description contains other info (datatype, etc.)
            dict_rows = [dict(zip(names, vals)) for vals in rows]    
        else:
            dict_rows = rows
            
        return dict_rows
        

    def findBlob(self):
        candidate = self.findFirst("select filename, filedata  from PDM_Candidate_plots where pdm_plot_type_id = 2;")
        #c2 = self.findFirst("select datalength(filedata) as b  from  PDM_Candidate_plots where pdm_plot_type_id = 2;")     
        file = open(candidate["filename"], 'wb')        
        file.write(candidate["filedata"])
        file.close()
        
        return candidate["filename"]

    
    def findBlobLimit(self, id):
        results = self.findAll("select top 15 pdm_cand_id from PDM_Candidate_plots where pdm_plot_type_id = 2 and pdm_cand_id > " + str(id) + ";")
        print results
        for result in results:
            candidate = self.findFirst("select filename, filedata  from PDM_Candidate_plots where pdm_plot_type_id = 2 and pdm_cand_id = " + str(result["pdm_cand_id"]) + ";")
            print candidate["filename"]
            file = open(candidate["filename"], 'wb')        
            file.write(candidate["filedata"])
            file.close()
        return candidate["filename"]    
   

if __name__=='__main__':
    if len(sys.argv) > 1:
        print "Connecting to", sys.argv[1]
        db = Database(sys.argv[1])
    else:
        print "Connecting to", DEFAULTDB
        db = Database()
    print "Connected!"
    print "conn:", db.conn
    print "cursor:", db.cursor
    db.close()

