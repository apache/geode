ifndef base
  base=../../../../../../..
endif

include $(base)/makefiles/platform.gmk
OBJDIR=$(OSBUILDDIR)/src/cpp_objects
LIBDIR=$(OSBUILDDIR)/hidden/lib/$(ARCH).$(OSNAME)
LIBDIR=$(OSBUILDDIR)/hidden/lib
LIBRARY = $(LIBDIR)/$(libPrefix)DHImpl$(libSuffix)

default: $(LIBRARY)

all: directories
	$(MAKE) VARIANT=fast

.PHONY: all
.PHONY: directories

LIBS=
LIB_PATH = -L$(LIBDIR) -L$(OPENSSL)/lib
INCLUDE=-I. -I$(OPENSSL)/include
OUT.c = -o $@
FILES_cpp := $(wildcard *.cpp)
FILES_o := $(foreach item, $(subst .cpp,$(OBJ),$(FILES_cpp)),$(OBJDIR)/$(item))

ifeq ($(HOSTTYPE_OSTYPE),$(filter $(HOSTTYPE_OSTYPE),sparc.Solaris x86.Solaris))
  CXX=CC
  LIBS += -lrt -lcrypto
  CFLAGS_COMMON = -mt -KPIC -D_REENTRANT -D_RWSTD_MULTI_THREAD -DTHREAD=MULTI -D_SOLARIS $(INCLUDE) -c
  # using -xMMD to fix issue with implicit rule to rebuild string from string.cc
  CFLAGS_COMMON += -xMMD
  LD_FLAGS = -mt $(CFLAGS_MODEL) -G -Bdynamic -KPIC -zdefs
  ifdef USE_CPP11
    CFLAGS_COMMON += -std=c++11
    LD_FLAGS += -std=c++11
    LIBS += -lstdc++ -lgcc_s -lCrunG3
  else
    LIBS += -lCstd -lCrun
  endif
  LIBS += -lc
else
ifeq ($(HOSTTYPE_OSTYPE),intel.Linux)
  CXX=g++
  LIBS +=  -lcrypto
  CFLAGS_COMMON = -D_REENTRANT -D_LINUX $(FPIC) $(INCLUDE) -c -MD
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
