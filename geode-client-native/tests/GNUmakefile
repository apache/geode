# This is invoked from ANT with many 
# variables defined on the command line.

default: all

#.PHONY: all

DIRS = userobject testobject security queryTests cacheserver event utils perfTests fwk fwklib fwkbin cppPerf durableClientTest functionExecution smokeperf delta multiusersecurity pdxtest

all: makeslow makefast

makefast:
	$(foreach item,$(DIRS),$(MAKE) -C $(item) $@ &&):
makeslow:
	$(foreach item,$(DIRS),$(MAKE) -C $(item) $@ &&):
	
