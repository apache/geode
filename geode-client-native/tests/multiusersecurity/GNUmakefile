# This is invoked from ANT with many 
# variables defined on the command line.

default: all

LIBRARY = multiusersecurity
CPP_OPTIONS =

XTRA_LIB_PATHS =
XTRA_LIBS = -ltestobject -lsecurity -lpdxobject
XTRA_INCS = -I$(LBMDIR)/include -I$(OPENSSL)/include

include $(BASEDIR)/makefiles/test_library.gmk
