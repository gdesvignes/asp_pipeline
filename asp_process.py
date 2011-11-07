import glob, aspfits, sys, os
import database as db
from optparse import OptionParser


ASPFILES = "/dataM1/BONn/asp/20??/"
PSRHOME = "/home/desvignes/pulsars/"
EPHEM = "/home/desvignes/pulsars/ephem"


full_usage = """
usage : asp_process.py [options] 

  [-h, --help]        : Display this help
  [-v, --verbose]     : Verbose mode
  [-p, --pulsar %s]   : Select pulsar to process
  [-L, --load]        : Load asp files into the database
  [-t, --tdiff %f]    : Discard all integrations after a TDIFF jump by this amount (default: 20 ns) 
  [-x, --transform]   : Do the transformation 

"""
usage = "usage: %prog [options]"

class ASP:

    def __init__(self, database, opts):
        self.database = database
	self.opts = opts
	self.psrname = opts.pulsar
        self.dest_dir = "%s/%s"%(PSRHOME, self.psrname)

    def connect(self):
        # Connect to DB
	self.db = db.Database(db = self.database)
	self.DBconn = self.db.conn
	self.DBcursor = self.db.cursor

    def close(self):
        self.DBconn.close()

    def zapping(self):	
        self.connect()
        query = "SELECT P.psrchive,P.pazi FROM asp_processing AS P INNER JOIN asp_headers as H ON P.filename= H.filename WHERE H.psrname = '%s' and H.mode='PSR';"%self.psrname
        self.DBcursor.execute(query)
        result_query = [list(row) for row in self.DBcursor.fetchall()]
        self.close()
	print result_query

	for psrchive, pazi in result_query:
	    zap_infos = os.popen("pazi %s/%s"%(self.dest_dir, psrchive))
	print zap_infos    
	sys.exit()

    def unload_asp_filenames(self):
        self.connect()
	query = "SELECT filename FROM asp_headers;"
	self.DBcursor.execute(query)
	asp_filenames = [list(row)[0] for row in self.DBcursor.fetchall()] 
	self.close()
	return asp_filenames

    def load_asp_to_database(self):

	print "Scanning ASP directories '%s'"%ASPFILES
	asp_files = glob.glob("%s/*.asp"%ASPFILES)
	asp_files.sort()

	# Get the filenames already loaded in the database
	asp_filenames = self.unload_asp_filenames()
	print "%d entries in the database"%len(asp_filenames)

	# Connect to the MySQL database
	self.connect()

	new_asp_files = []
	for asp_file in asp_files:
	    if os.path.split(asp_file)[1] not in asp_filenames:
		new_asp_files.append(asp_file)
	    else:	
		filename = os.path.split(asp_file)[1]
	        query = "SELECT period from asp_headers WHERE filename = '%s';"%(filename)
		self.DBcursor.execute(query)
		result_query = self.DBcursor.fetchone()[0]

		if result_query:
		    continue

		asp = aspfits.ASPfits(asp_file)
		if asp.get_period():
		    query = "UPDATE asp_headers SET period=%f WHERE filename = '%s';"%(asp.get_period(), filename)
		else:
		    continue
		print query
		self.DBcursor.execute(query)
		asp.close()
		del asp


	for ii,asp_file in enumerate(new_asp_files):
 
	    print "Loading '%s' (%d/%d)"%(asp_file, ii+1, len(new_asp_files))

	    # Read asp infos from files
	    try:
	        asp = aspfits.ASPfits(asp_file)
	    except:
	        continue 
	    freq = asp.get_obsfreq()
	    mjd = asp.get_mjd()

	    # Correct for frequency offset
	    if 1350.0 < freq  and freq < 1450.0 and mjd < 53686.05:
		    freq = freq - 1.0

	    path = os.path.split(asp_file)[0]
	    filename = os.path.split(asp_file)[1]


	    query = "INSERT IGNORE INTO asp_headers (filename, psrname, mode, period, DM, mjd, freq, bandwidth, nchan, t_int, t_fold, n_dump, path) VALUES ('%s', '%s', '%s', %f, %f, %f, %f, %f, %d, %f, %f, %d, '%s');"%(filename, asp.get_srcname(), asp.get_mode(), asp.get_period(), asp.get_DM(), mjd, freq,\
	    asp.get_bw(), asp.get_nchan(), asp.get_tint(), asp.get_tfold(),\
	    asp.get_ndump(), path)
	    #print query
	    self.DBcursor.execute(query)

	    query = "INSERT IGNORE INTO asp_processing (filename) VALUES ('%s');"%(filename)
	    #print query
	    self.DBcursor.execute(query)


	    asp.close()
	    del asp

	# Clock files    
	print "Scanning directories '%s' for clock files"%ASPFILES
	clk_files = glob.glob("%s/*.clk*"%ASPFILES)
	clk_files.sort()
	print "Found %d CLK files"%len(clk_files)
	for clk_file in clk_files:

	    query = "UPDATE asp_processing SET clkfile='%s' WHERE filename = '%s.asp';"%(clk_file, os.path.split(clk_file)[1][:22])
	    #print query
	    self.DBcursor.execute(query)

	self.close()

    def find_cal_for_obs(self, aspfile, src="", freq=1398, mjd=55000):
        """
	Search for the corresponding CAL file
	for a PSR obs, given either the ASP filename or
	'psrname', 'freq', 'MJD'
	"""
	self.connect()
	if aspfile:
	    asp = aspfits.ASPfits(aspfile)
	    src = asp.get_srcname()
	    freq = asp.get_obsfreq()
	    mjd = asp.get_mjd()
	    asp.close()
	    del asp
	query = "SELECT path,filename FROM asp_headers WHERE mode='CAL' AND psrname='%s' AND freq='%s' AND ABS(mjd-%f)<0.5;"%(src,freq,mjd)
	self.DBcursor.execute(query)
	result_query = self.DBcursor.fetchone()
	self.close()
	if result_query:
	    return os.path.join(result_query[0],result_query[1])
	else:
	    return result_query

    def check_file_in_db(self, selectfield, table, field, file):
	"""
	Check if a file has already been put in this tqble
	Return True if file should be reprocessed
	Return Flase if the processing should be skipped
	"""
	self.connect()
	query = "SELECT H.%s FROM %s AS H WHERE H.%s = '%s';"%(selectfield, table, field, file)
        if self.opts.verbose:
	    print query
	self.DBcursor.execute(query)
	found_file = self.DBcursor.fetchone()[0]
	self.close()

	if found_file:
	    if self.opts.verbose:
		print "Found '%s' in the DB"%found_file
	    if os.path.isfile(os.path.join(self.dest_dir, found_file)):
		return False
		# Force to write files but delete previous first
		if self.opts.force:
		    cmd = "rm -rf %s*"%os.path.join(self.dest_dir, found_file)
		    os.system(cmd)
		    return True
	    else:
		return True
	else:
	    return True

    def update_field(self, file, table, field, new_val):
        """
        Insert if a file has already been put in this tqble
        """
        self.connect()
	if type(new_val).__name__=='float':
	    query = "UPDATE %s SET %s=%f WHERE filename = '%s';"%(table, field, new_val, file)
	else:
	    query = "UPDATE %s SET %s='%s' WHERE filename = '%s';"%(table, field, new_val, file)
        if self.opts.verbose:
            print query
        self.DBcursor.execute(query)
        self.close()

    """
    def asp2aspc(self, tdiff_limit):
        ""
        Convert ASP file into psrchive format
        unless a file is already written in the DB
        ""
        query = "SELECT H.path, H.filename, H.mode FROM asp_headers AS H WHERE H.psrname = '%s';"%self.psrname
        self.connect()
        self.DBcursor.execute(query)
        result_query = [list(row) for row in self.DBcursor.fetchall()]
        self.close()
        #print result_query
        if not result_query:
            print "PSR %s not found in the database"%self.psrname
            sys.exit(0)

        # DEstination Dir
        try:
            os.mkdir(self.dest_dir)	    
        except:
            pass

        # Adapt to psrchive
        for path, file, mode in result_query:
            obsfile = os.path.join(path, file)

            # Determine if we process the file
	    #  - If aspc file not in DB, process
	    #  - If force, process
	    #  - If file don't exist, process
            do_asp2aspc = self.check_file_in_db("aspc", "asp_processing", "filename", file)

            # Do the conversion
            if do_asp2aspc:
                cmd = "asp2aspc %s -d %s -t %f"%(obsfile, self.dest_dir, tdiff_limit)
                self.update_field(file, "asp_processing", "tdiff", tdiff_limit)
		# Search for CAL files if this is a PSR file
		if mode=='PSR':
		    calfile = self.find_cal_for_obs(obsfile)
		    if self.opts.verbose:
			print "asp_pipeline::find_cal_for_obs> Found '%s'"%calfile
		    if calfile:
			cmd = cmd + " -c %s"%calfile

                if self.opts.verbose:
                    print cmd
                os.system(cmd)

	    # Update the DB with the new processed files
	    if mode=='PSR':
		aspc_file = glob.glob(os.path.join(self.dest_dir,file.replace(".asp",".aspc*")))
	    elif mode=='CAL':	
	        aspc_file = glob.glob(os.path.join(self.dest_dir,file.replace(".asp",".asp.cl")))
	    try:
	        aspc_file = os.path.split(aspc_file[0])[1] 
	        self.update_field(file, "asp_processing", "aspc", aspc_file)
	    except:
	        self.update_field(file, "asp_processing", "aspc", "NULL")
    """

    def par2tempo2(self):
        cmd = "tempo2 -gr transform %s/%s.par %s/%s.par-t2"%(EPHEM,self.psrname,EPHEM,self.psrname)
	os.system(cmd)

    def asp2psrfits(self):

        query = "SELECT P.filename, H.mode, H.path FROM asp_headers AS H INNER JOIN asp_processing AS P ON H.filename=P.filename WHERE H.psrname = '%s';"%self.psrname
	#print query
        self.connect()
        self.DBcursor.execute(query)
        result_query = [list(row) for row in self.DBcursor.fetchall()]
        self.close()

        # DEstination Dir
        try:
            os.mkdir(self.dest_dir)	    
        except:
            pass


	for filename, mode, path in result_query:
            # If aspc file not already in DB, process
            do_asp2psrfits = self.check_file_in_db("psrfits", "asp_processing", "filename", filename)

            # Do the conversion
            if do_asp2psrfits:

		if mode == 'PSR':
		    cmd = "pam -e ar -E %s/%s.par-t2 %s/%s -u %s"%(EPHEM,self.psrname, path,filename,self.dest_dir)
		elif mode == 'CAL':
		    cmd = "pam -e pcal %s/%s -u %s"%(path,filename,self.dest_dir)
		if self.opts.verbose:
		    print cmd
		os.system(cmd)	
		    
		# Update the DB with the new processed files
		try:
		    psrfits_file = (glob.glob(os.path.join(self.dest_dir,"%s.ar"%filename[:filename.rindex(".")])) + glob.glob(os.path.join(self.dest_dir,"%s.pcal"%filename[:filename.rindex(".")])))
		    psrfits_file = psrfits_file[0]
		except:
		    psrfits_file = 'NULL'

		if mode == 'CAL':
		    cmd = "/home/desvignes/bin/set_cal_nstate %s"%psrfits_file
		    if self.opts.verbose:
			print cmd
		    os.system(cmd)	
		psrfits_file = os.path.split(psrfits_file)[1]
		if self.opts.verbose:
		    print "asp2psrfits> Found %s"%psrfits_file 
		self.update_field(filename, "asp_processing", "psrfits", psrfits_file)


    def pac_calib(self):

        query = "SELECT P.filename,P.psrfits FROM asp_headers AS H INNER JOIN asp_processing AS P ON H.filename=P.filename WHERE H.psrname = '%s' AND H.mode='PSR';"%self.psrname
	#print query
        self.connect()
        self.DBcursor.execute(query)
        result_query = [list(row) for row in self.DBcursor.fetchall()]
        self.close()

	# Move to the directory
	os.chdir(self.dest_dir)
	# Create a database of calibration
	cmd = "pac -w"
	os.system(cmd)	

	for filename, psrchivefile in result_query:
            # If aspc file not already in DB, process
	    if self.opts.verbose:
		print "pac_calib> Checking for previously calibrated %s"%psrchivefile 
            do_pac_calib = self.check_file_in_db("pac_calibrated", "asp_processing", "psrfits", psrchivefile)

            # Do the conversion
            if do_pac_calib:

		# Try to calibrate
		cmd = "pac -T -d database.txt %s"%(psrchivefile)
		if self.opts.verbose:
		    print cmd
		os.system(cmd)	
		    
		# Update the DB with the new processed files
		try:
		    if self.opts.verbose:
			print "pac_calib> Looking for the newly calibrated file %s in %s"%(psrchivefile, s.path.join(self.dest_dir,"%s.cali*"%psrchivefile[:psrchivefile.rindex(".")]))
		    pac_calibrated_file = glob.glob(os.path.join(self.dest_dir,"%s.cali*"%psrchivefile[:psrchivefile.rindex(".")]))[0]
		    pac_calibrated_file = os.path.split(pac_calibrated_file)[1]
		except:
		    pac_calibrated_file = "None"

		# Manual calibration using normalize_archive
		if pac_calibrated_file == "None":
		    cmd = "normalize_archive %s -e calibM"%psrchivefile
		    os.system(cmd)
		    if glob.glob(os.path.join(self.dest_dir,"%s.cali*"%psrchivefile[:psrchivefile.rindex(".")]))[0]:
		        pac_calibrated_file = os.path.split(pac_calibrated_file)[1]

		if self.opts.verbose:
		    print "pac_calib> Will put %s as the calibrated file in the DB"%pac_calibrated_file
		self.update_field(filename, "asp_processing", "pac_calibrated", pac_calibrated_file)


def main():
    parser = OptionParser(usage)

    parser.add_option("-L", "--load", action="store_true", dest="loadasp",
    			default=False, help="Load asp files into the database")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose",
    			default=False, help="Verbose mode")
    parser.add_option("-f", "--force", action="store_true", dest="force",
    			default=False, help="Force to reprocess")
    parser.add_option("-x", "--transform", action="store_true", dest="transform",
    			default=False, help="Transform into psrchive archives")
    parser.add_option("-z", "--zapping", action="store_true", dest="zapping",
    			default=False, help="Zap")
    parser.add_option("-p", "--pulsar", type="string", dest="pulsar", default=None,
                          help="Select pulsar to process")
    #parser.add_option("-t", "--tdiff", type="float", dest="tdiff", default=20.0,
    #                      help="Discard all integrations after a TDIFF jump by this amount (default: 20ns )")

    (opts, args) = parser.parse_args()
    if len(sys.argv) < 2:
        print full_usage
        #sys.exit(0)

    Asp = ASP("ASP", opts)

    # Load asp files into the database
    if opts.loadasp:	
        Asp.load_asp_to_database()

    # Convert in psrchive format
    if opts.pulsar and opts.transform:
        #Asp.asp2aspc(opts.tdiff)
        Asp.par2tempo2()
	Asp.asp2psrfits()
	Asp.pac_calib()

    # Zapping 
    if opts.pulsar and opts.zapping:
        Asp.zapping()

if __name__ == '__main__':

    main()


