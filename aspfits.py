import sys
import pyfits

class ASPfits:

    def __init__(self, filename):
        " Comment"    
	self.hdu = pyfits.open(filename)

        # Header
	self.hdr = self.hdu[0]
	self.beconfig = self.hdu[1]
	self.cohddisp = self.hdu[2]

	self.nprof = len(self.hdu) - 3

	"""
	print self.hdr.header
	print self.hdr.data
	print self.beconfig.header
	print self.beconfig.data
	print "----------------------------"
	print self.cohddisp.header
	print self.cohddisp.data
	print self.hdu[3].header
	print self.hdu[3].data
	"""

    def close(self):
	del self.hdr
        del self.beconfig
	del self.cohddisp
        self.hdu.close()

    def get_mode(self):
        return self.hdr.header['OBS_MODE']

    def get_srcname(self):
        return self.hdr.header['SRC_NAME']

    def get_DM(self):
	return self.cohddisp.data[0][0]

    def get_period(self):
        try:
	    return self.hdu[3].header['HIERARCH DUMPREFPER']
	except:
	    return None

    def get_tint(self):
        return self.hdr.header['SCANLEN']/self.hdr.header['NDUMPS']* self.nprof

    def get_obsfreq(self):
        return self.hdr.header['FSKYCENT']

    def get_ndump(self):
        return self.nprof

    def get_tfold(self):
        return self.hdr.header['SCANLEN']/self.hdr.header['NDUMPS'] 

    def get_bw(self):
        return self.beconfig.data[0][1+self.beconfig.data[0][0]]

    def get_nchan(self):
	return self.beconfig.data[0][0]

    def get_mjd(self):
	return self.hdr.header['STT_IMJD'] + self.hdr.header['STT_SMJD']/86400.0 

    def list_values(self):
        print " SRCNAME :", self.get_srcname()
        print "    MODE :", self.get_mode()
	print "      DM :", self.get_DM()
	print "  PERIOD :", self.get_period()
	print "    TINT :", self.get_tint()
	print " OBSFREQ :", self.get_obsfreq()
	print "   NDUMP :", self.get_ndump()
	print "   TFOLD :", self.get_tfold()
	print "      BW :", self.get_bw()
	print "   NCHAN :", self.get_nchan()
	print "     MJD :", self.get_mjd()


if __name__ == '__main__':

    pfits = ASPfits(sys.argv[1])
    pfits.list_values()

