ifndef base
  base=../../../../../../..
endif

include $(base)/makefiles/platform.gmk
OBJDIR=$(OSBUILDDIR)/src/bdb_objects
LIBDIR=$(OSBUILDDIR)/product/lib/$(ARCH).$(OSNAME)
LIBDIR=$(OSBUILDDIR)/product/lib
LIBRARY = $(LIBDIR)/$(libPrefix)BDBImpl$(libSuffix)

default: $(LIBRARY)

all: directories
	$(MAKE) VARIANT=fast

.PHONY: all
.PHONY: directories

LIBS=$(ACE_DIR)/lib/libACE.a -lgfcppcache -ldb_cxx
LIB_PATH = -L$(LIBDIR) -L$(BDB)/lib
INCLUDE=-I. -I$(ACE_DIR)/include -I$(base)/src/com/gemstone/gemfire/internal/cppcache -I$(base)/src/com/gemstone/gemfire/internal/cppcache/impl -I$(BDB)/include
OUT.c = -o $@
FILES_cpp := $(wildcard *.cpp)
FILES_o := $(foreach item, $(subst .cpp,$(OBJ),$(FILES_cpp)),$(OBJDIR)/$(item))

ifeq ($(HOSTTYPE_OSTYPE),$(filter $(HOSTTYPE_OSTYPE),sparc.Solaris x86.Solaris))
  CXX=CC
  LIBS += -lrt -lsocket -lm -lnsl
  CFLAGS_COMMON = -mt -KPIC -D_REENTRANT -D_RWSTD_MULTI_THREAD -DTHREAD=MULTI -D_SOLARIS $(INCLUDE) -c
  # using -xMMD to fix issue with implicit rule to rebuild string from string.cc
  CFLAGS_COMMON += -xMMD
  LD_FLAGS = -KPIC -mt $(CFLAGS_MODEL) -G -Bdynamic -z defs
  ifdef USE_CPP11
    CFLAGS_COMMON += -std=c++11
    LD_FLAGS += -std=c++11
    LIBS += -lstdc++ -lgcc_s -lCrunG3 
  else
    LIBS += -lCstd -lCrun 
  endif
  LIBS += -lc

ifeq ($(HOSTTYPE_OSTYPE),sparc.Solaris)
  CFLAGS_COMMON += -D_SPARC_SOLARIS
else
ifeq ($(HOSTTYPE_OSTYPE),x86.Solaris)
  CFLAGS_COMMON += -D_X86_SOLARIS
endif
endif
else
ifeq ($(HOSTTYPE_OSTYPE),intel.Linux)
  CXX=g++
  CFLAGS_COMMON = -D_REENTRANT -D_LINUX $(INCLUDE) $(FPIC) -c -MD
  LD_FLAGS = $(CFLAGS_MODEL) -shared
endif
endif
CFLAGS_fast=-O3 $(CFLAGS_COMMON)
CFLAGS_slow=-O0 -g $(CFLAGS_COMMON)

COMPILE.cpp = $(CXX) $(CFLAGS_MODEL) $(CFLAGS_$(VARIANT))

$(OBJDIR)/%$(OBJ): %.cpp
	@echo compiling $(VARIANT) $<
	$(COMPILE.cpp) $(OUT.c) $(call NATIVEDIR_FUNC,$<)

$(LIBRARY): $(FILES_o)
	$(CXX) $(LD_FLAGS) -o $@ $(FILES_o) $(LIB_PATH) $(LIBS)

directories: $(call UNIXDIR_FUNC,$(OBJDIR))

@mkdir -p $(call UNIXDIR_FUNC,$(OBJDIR)):
	@mkdir -p $(call UNIXDIR_FUNC,$(OBJDIR))

-include $(FILES_o:.o=.d)
