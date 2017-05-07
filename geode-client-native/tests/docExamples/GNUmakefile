ifndef base
  base = ../..
endif

ifndef VARIANT
  VARIANT=fast
endif

DIRS = 

default: test_targets

all: 
	$(foreach item,$(DIRS),$(MAKE) -C $(item) $@ &&):
	$(MAKE) VARIANT=fast


.PHONY: all test_targets postinclude_targets

include $(base)/makefiles/cppcache.gmk

DESTDIR=$(OSBUILDDIR)/tests/docExamples
GENDIR=$(OSBUILDDIR)/tests/docExamples
OBJDIR=$(DESTDIR)

PRODDIR=$(OSBUILDDIR)/product

INTDIR=$(base)/src/com/gemstone/gemfire/internal/cppcache
INTERNALDIR=$(base)/src/com/gemstone/gemfire/internal/
CINCL=-I$(product)/include -I$(INTERNALDIR) -I$(INTDIR)/impl -I$(INTDIR) -I$(DESTDIR) -I$(ACE_DIR)/include -I$(GENDIR) -I$(base)/tests

include $(base)/makefiles/executable.gmk

CFLAGS += -D__ACE_INLINE__ -DACE_NLOGGING -DACE_NDEBUG -D__COMPILE_DUNIT_

ifeq ($(CPPDEVEL),1)
  CFLAGS += -DGF_DEVEL_ASSERTS=1
endif

ifeq ($(HOSTTYPE_OSTYPE),$(filter $(HOSTTYPE_OSTYPE),sparc.Solaris x86.Solaris))
  LDLIBS = -mt -L $(PRODDIR_lib) -lpthread -lrt
  INCSTLPORT = -I$(STLPORT)/stlport
  CFLAGS += -DSOLARIS -D_REENTRANT
LDLIBS += -L$(PRODDIR_lib)/../../hidden/lib -lgfcppcache -L$(FRAMEWORK)/lib -l$(TESTOBJECTLIB) -lframework -l$(ACELINKNAME)
  DLLLIBS += -L$(PRODDIR_lib) -L$(PRODDIR_lib)/../../hidden/lib -lgfcppcache -l$(ACELINKNAME) -lpthread
  ifeq ($(CPPDEBUG),true)
    CFLAGS += -g 
  else
    CFLAGS += -O3
  endif
  ifdef USE_CPP11
    CFLAGS += -std=c++11
    LDLIBS += -std=c++11
  else
    CFLAGS += -library=no%Cstd,no%iostream $(INCSTLPORT)
    LDLIBS += -library=no%Cstd,no%iostream
  endif

else
ifeq ($(HOSTTYPE_OSTYPE),intel.Linux)
  LDLIBS = -L $(PRODDIR_lib) -lpthread
  INCSTLPORT=-I$(STLPORT)/stlport
  SH_LIB = /gcm/where/cplusplus/smartheap/x86.Linux/lib
  SH_INC = /gcm/where/cplusplus/smartheap/x86.Linux/include
  ifeq ($(USE_SMARTHEAP),true)
    ifeq ($(CPPDEBUG),true)
      SH_SFX = _mtd.a
    else
      SH_SFX = _mt.a
    endif
    CFLAGS += -I$(SH_INC) -DUSE_SMARTHEAP
    SH_LD_ARGS = $(SH_LIB)/libsmartheapC$(SH_SFX) $(SH_LIB)/libsmartheap$(SH_SFX)
  else
    SH_LD_ARGS = 
  endif

LDLIBS += $(SH_LD_ARGS) -rdynamic -L$(PRODDIR_lib)/../../hidden/lib -lgfcppcache -L$(FRAMEWORK)/lib -rdynamic -lxerces-c -l$(TESTOBJECTLIB) -lframework -l$(ACELINKNAME) -lpthread -rdynamic -lstdc++
  DLLLIBS += $(SH_LD_ARGS) -rdynamic -L$(PRODDIR_lib) -L$(PRODDIR_lib)/../../hidden/lib -lgfcppcache -l$(ACELINKNAME) -lpthread -rdynamic -lstdc++
  CFLAGS += $(INCSTLPORT) -D_REENTRANT -Werror
  ifeq ($(CPPDEBUG),true)
    CFLAGS += -g 
  else
    CFLAGS += -O3
  endif

else
  INCSTLPORT=-I'$(STLPORT)/stlport'
  CFLAGS_COMMON = -nologo /GR /EHac /DWINVER=0x0500 /D_WIN32_WINNT=0x0500 -W3 -D_CRT_SECURE_NO_DEPRECATE -D_CRT_NONSTDC_NO_DEPRECATE $(INCSTLPORT) 
  LDLIBS = /DEBUG /LIBPATH:$(call NATIVEDIR_FUNC,$(FRAMEWORK)/lib) gfcppcache.lib $(ACELINKNAME).lib testobject.lib /MAP
  DLLLIBS = /DEBUG /LIBPATH:$(call NATIVEDIR_FUNC,$(PRODDIR_lib)) /LIBPATH:$(call NATIVEDIR_FUNC,$(PRODDIR_lib)/../../hidden/lib) gfcppcache.lib $(ACELINKNAME).lib /MAP
  ifeq ($(CPPDEBUG),true)
    CFLAGS += $(CFLAGSslow)
  else
    CFLAGS += $(CFLAGSfast)
  endif

endif
endif


TESTS_CPP_FILES := $(sort $(wildcard *.cpp))

TARGETS := $(foreach item,$(subst .cpp,${EXE},$(TESTS_CPP_FILES)),$(DESTDIR)/$(item))
#ifeq ($(VCVER),vc8)
 # # This is needed so that the debug executables can load the non-debug dll
  #ifeq ($(GFLIB_MODEL),64bit)
  #MANIFEST_DEPENDENCY += 
  #else
  #MANIFEST_DEPENDENCY += /manifestdependency:"type='win32' name='Microsoft.VC80.CRT' version='8.0.50608.0' processorArchitecture='X86' publicKeyToken='1fc8b3b9a1e18e3b'"
  #endif
#endif

test_targets: $(TARGETS)

clean:
	rm -f $(TARGETS) *${OBJ} *.log core

  
  
