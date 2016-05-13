# This is invoked from ANT with many 
# variables defined on the command line.

default: all

LIBRARY = security
CPP_OPTIONS =

XTRA_LIB_PATHS = -L$(OPENSSL)/lib
XTRA_LIBS = -ltestobject -lcrypto -lpdxobject
XTRA_INCS = -I$(LBMDIR)/include -I$(OPENSSL)/include

include $(BASEDIR)/makefiles/test_library.gmk
