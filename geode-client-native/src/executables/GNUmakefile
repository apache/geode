ifndef base
  base = ../..
endif

# for each file listed, an exe will be generated.
EXE_CPP_FILES = \
  gfcpp.cpp \
  

ifndef VARIANT
  VARIANT=fast
endif

default: postinclude_targets


all: 
	$(MAKE) VARIANT=fast


.PHONY: all postinclude_targets

include $(base)/makefiles/cppcache.gmk

DESTDIR=$(OSBUILDDIR)/src/executables
GENDIR=$(OSBUILDDIR)/src/executables
OBJDIR=$(DESTDIR)

PRODDIR=$(OSBUILDDIR)/product

make_exe_directories:
	mkdir -p $(OBJDIR)

INTDIR=$(base)/src/com/gemstone/gemfire/internal/cppcache
INTERNALDIR=$(base)/src/com/gemstone/gemfire/internal/
CINCL=-I$(product)/include -I$(INTERNALDIR) -I$(INTDIR)/impl -I$(INTDIR) -I$(DESTDIR) -I$(ACE_DIR)/include -I$(GENDIR)

include $(base)/makefiles/executable.gmk

ifeq ($(HOSTTYPE_OSTYPE),sparc.Solaris)
LDLIBS = -L -lpthread $(LIBS) 
INCSTLPORT=-I$(STLPORT)/stlport
else
ifeq ($(HOSTTYPE_OSTYPE),x86.Solaris)
LDLIBS = -L -lpthread $(LIBS) 
#LDLIBS = -L -lpthread $(LIBS) 
INCSTLPORT=-I$(STLPORT)/stlport
else
ifeq ($(HOSTTYPE_OSTYPE),intel.Linux)
LDLIBS = -L $(PRODDIR_lib) -lpthread $(LIBS) 
INCSTLPORT=-I$(STLPORT)/stlport
else
INCSTLPORT=-I'$(STLPORT)/stlport'
CFLAGS_COMMON = -nologo /Zi /GR /EHac /DWINVER=0x0500 /D_WIN32_WINNT=0x0500 -W3 $(INCSTLPORT)
endif
endif
endif
CFLAGS += -D__ACE_INLINE__ -DACE_NLOGGING -DACE_NDEBUG

ifeq ($(HOSTTYPE_OSTYPE),$(filter $(HOSTTYPE_OSTYPE),sparc.Solaris x86.Solaris))
  CFLAGS += -mt -D_REENTRANT
    
  ifeq ($(GFLIB_MODEL),64bit)
    LCX =
    #TODO jbarrett CFLAGS += -xmemalign=4s
  else
#TODO jbarrett - really??
#    LCX = -lcx
  endif
  LDLIBS += -mt -lgfcppcache $(LCX)
  
  ifdef USE_CPP11
    CFLAGS += -std=c++11
    LDLIBS += -std=c++11
  else
    #CFLAGS += -library=no%Cstd,no%iostream $(INCSTLPORT)
    #LDLIBS += -library=no%Cstd,no%iostream
    CFLAGS += $(INCSTLPORT)
    LDLIBS += 
  endif

  CFLAGS += -O3
  CFLAGS += -KPIC
else
ifeq ($(HOSTTYPE_OSTYPE),intel.Linux)
  LDLIBS += -lgfcppcache$(libSize) -rdynamic -lstdc++
  CFLAGS += $(INCSTLPORT) -D_REENTRANT
  CFLAGS += -O3
else
  CFLAGS += $(CFLAGSfast) $(CFLAGS$(VCVER))
  LDLIBSfast = $(ACELINKNAME).lib
  LDLIBSslow = $(ACELINKNAME)d.lib
  LDLIBS = gfcppcache.lib $(LDLIBS$(VARIANT)) Advapi32.lib /MAP
endif
endif

TARGETS := $(foreach item,$(subst .cpp,${EXE},$(EXE_CPP_FILES)),$(DESTDIR)/$(item))

postinclude_targets: make_exe_directories $(TARGETS)


clean:
	rm -f $(TARGETS) *${OBJ} *.log core
