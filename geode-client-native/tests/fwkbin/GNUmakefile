# This is invoked from ANT with many 
# variables defined on the command line.

default: all

SUITE = fwkbin

CPP_OPTIONS = -I$(BASEDIR)/tests/fwklib

BUILD_EXES = Driver Client FileProcessor Validate FwkBB TimeStr

include $(BASEDIR)/makefiles/test_executables.gmk
