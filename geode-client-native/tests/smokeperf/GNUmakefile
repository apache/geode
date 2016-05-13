# This is invoked from ANT with many 
# variables defined on the command line.

default: all

LIBRARY = smokeperf
CPP_OPTIONS =

XTRA_LIB_PATHS =
XTRA_LIBS = -lsecurity -ltestobject -lpdxobject
XTRA_INCS = -I$(LBMDIR)/include

include $(BASEDIR)/makefiles/test_library.gmk
