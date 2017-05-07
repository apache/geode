ifndef base
 base = ../../../../../..
endif

LIBRARY = gfcppcache

# Add to FILES_cpp for all .cpp files in this dir.
FILES_cpp := $(sort $(wildcard *.cpp))

ifeq ($(OSNAME),SunOS)
  UNAME_P := $(shell uname -p)
  ifeq ($(UNAME_P),sparc)
    FILES_asm := impl/hostsolaris.asm
  else
    FILES_asm :=
  endif
else
  FILES_asm :=
endif

IMPL_FILES_cpp := $(sort $(wildcard impl/*.cpp))
STATISTICS_FILES_cpp := $(sort $(wildcard statistics/*.cpp))

# following files listed separately so we can control their position
#  in linker input arguments .
#
FILES_LAST_c = 
FILES_FIRST_c = 


#CPPCACHE_INCLUDES = \
# -I$(INTERNAL_SRC_DIR)/cppcache \
# -I$(INTERNAL_SRC_DIR)/cppcache/statistics \
# -I$(INTERNAL_SRC_DIR)/cppcache/impl


include $(base)/makefiles/cppcache.gmk

-include $(FILES_o:.o=.d)
