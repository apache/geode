ifndef base
  base = ../..
endif

ifndef VARIANT
  VARIANT=fast
endif

DIRS = 

default: test_targets postinclude_targets

all: 
	$(foreach item,$(DIRS),$(MAKE) -C $(item) $@ &&):
	$(MAKE) VARIANT=fast


.PHONY: all test_targets postinclude_targets

include $(base)/makefiles/cppcache.gmk

DESTDIR=$(OSBUILDDIR)/tests/cppcache
GENDIR=$(OSBUILDDIR)/tests/cppcache
OBJDIR=$(DESTDIR)

PRODDIR=$(OSBUILDDIR)/product

INTDIR=$(base)/src/com/gemstone/gemfire/internal/cppcache
INTERNALDIR=$(base)/src/com/gemstone/gemfire/internal/
CINCL=-I$(product)/include -I$(INTERNALDIR) -I$(INTDIR)/impl -I$(INTDIR) -I$(DESTDIR) -I$(ACE_DIR)/include -I$(GENDIR) -I$(base)/tests

include $(base)/makefiles/executable.gmk

ifeq ($(HOSTTYPE_OSTYPE),$(filter $(HOSTTYPE_OSTYPE),sparc.Solaris x86.Solaris))
LDLIBS = -L $(PRODDIR_lib) -lpthread -lrt
INCSTLPORT=-I$(STLPORT)/stlport
else
ifeq ($(HOSTTYPE_OSTYPE),intel.Linux)
LDLIBS = -L $(PRODDIR_lib) -lpthread
INCSTLPORT=-I$(STLPORT)/stlport
else
INCSTLPORT=-I'$(STLPORT)/stlport'
CFLAGS_COMMON = -nologo /GR /EHac 
CFLAGS_COMMON += /DWINVER=0x0500 /D_WIN32_WINNT=0x0500 
CFLAGS_COMMON += -W3 -D_CRT_SECURE_NO_DEPRECATE -D_CRT_NONSTDC_NO_DEPRECATE $(INCSTLPORT) 
endif
endif

CFLAGS += -D__ACE_INLINE__ -DACE_NLOGGING -DACE_NDEBUG -D__COMPILE_DUNIT_
EXTRA_OBJS = $(DESTDIR)/fw_dunit${OBJ}

ifeq ($(CPPDEVEL),1)
  CFLAGS += -DGF_DEVEL_ASSERTS=1
endif

ifeq ($(HOSTTYPE_OSTYPE),$(filter $(HOSTTYPE_OSTYPE),sparc.Solaris x86.Solaris))
  CFLAGS += -mt -DSOLARIS -D_REENTRANT 
  LDFLAGS = $(patsubst -library=%,,$(CFLAGS))
  CFLAGS += -KPIC
  EXTRA_OBJS += $(DESTDIR)/BBNamingContext${OBJ}
  LDLIBS += -L$(PRODDIR_lib)/../../hidden/lib -lgfcppcache -lnsl -l$(ACELINKNAME) -L$(FRAMEWORK)/lib -lframework -l$(TESTOBJECTLIB)
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
    CFLAGS += $(INCSTLPORT)
    LDLIBS += 
    #TODO jbarrett CFLAGS += -library=no%Cstd,no%iostream $(INCSTLPORT)
    #LDLIBS += -library=no%Cstd,no%iostream
  endif
  CALLBACKS=$(DESTDIR)/libunit_test_callbacks.so
else
ifeq ($(HOSTTYPE_OSTYPE),intel.Linux)
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
  EXTRA_OBJS += $(DESTDIR)/BBNamingContext${OBJ}
  LDLIBS += $(SH_LD_ARGS) -rdynamic -L$(PRODDIR_lib)/../../hidden/lib -lgfcppcache -l$(ACELINKNAME) -L$(FRAMEWORK)/lib -rdynamic -lframework -lxerces-c -l$(TESTOBJECTLIB) -lpthread -rdynamic -lstdc++
  DLLLIBS += $(SH_LD_ARGS) -rdynamic -L$(PRODDIR_lib) -L$(PRODDIR_lib)/../../hidden/lib -lgfcppcache -l$(ACELINKNAME) -lpthread -rdynamic -lstdc++
  CFLAGS += $(INCSTLPORT) -D_REENTRANT -Werror
  ifeq ($(CPPDEBUG),true)
    CFLAGS += -g 
  else
    CFLAGS += -O3
  endif
  CALLBACKS=$(DESTDIR)/libunit_test_callbacks.so
else
  LDLIBS = /DEBUG /LIBPATH:$(call NATIVEDIR_FUNC,$(FRAMEWORK)/lib) gfcppcache.lib $(ACELINKNAME).lib testobject.lib /MAP
  DLLLIBS = /DEBUG /LIBPATH:$(call NATIVEDIR_FUNC,$(PRODDIR_lib)) /LIBPATH:$(call NATIVEDIR_FUNC,$(PRODDIR_lib)/../../hidden/lib) gfcppcache.lib $(ACELINKNAME).lib /MAP
  ifeq ($(CPPDEBUG),true)
    CFLAGS += $(CFLAGSslow)
  else
    CFLAGS += $(CFLAGSfast)
  endif
  CALLBACKS=$(DESTDIR)/unit_test_callbacks.dll
endif
endif

TESTS_CPP_FILES := $(sort $(wildcard test*.cpp))

TARGETS := $(foreach item,$(subst .cpp,${EXE},$(TESTS_CPP_FILES)),$(DESTDIR)/$(item))
#ifeq ($(VCVER),vc8)
  # This is needed so that the debug executables can load the non-debug dll
 # ifeq ($(GFLIB_MODEL),64bit)
  #  MANIFEST_DEPENDENCY += /manifestdependency:"type='win32' name='Microsoft.VC80.CRT' version='8.0.50608.0' processorArchitecture='amd64' publicKeyToken='1fc8b3b9a1e18e3b'"
  #else
   # MANIFEST_DEPENDENCY += /manifestdependency:"type='win32' name='Microsoft.VC80.CRT' version='8.0.50608.0' processorArchitecture='X86' publicKeyToken='1fc8b3b9a1e18e3b'"
  #endif
#endif

ifneq ($(HOSTTYPE_OSTYPE),intel.Windows)
CFLAGS_NOSTLPORT = $(patsubst -library=%,,$(subst $(INCSTLPORT),,$(CFLAGS)))
$(DESTDIR)/BBNamingContext${OBJ}: BBNamingContext.hpp BBNamingContext.cpp
	$(CXX) $(CFLAGS_NOSTLPORT) -c BBNamingContext.cpp $(OUT.c)
endif

postinclude_targets: $(DESTDIR)/LibraryCallbacks${OBJ} $(CALLBACKS)
test_targets: $(TARGETS)
$(TARGETS): $(EXTRA_OBJS)

$(CALLBACKS): TallyListener.hpp TallyLoader.hpp TallyWriter.hpp LibraryCallbacks.cpp $(DESTDIR)/LibraryCallbacks${OBJ}
ifeq ($(HOSTTYPE_OSTYPE),sparc.Solaris)
	CC -G $(LDFLAGS) $(DESTDIR)/LibraryCallbacks${OBJ} -o $(CALLBACKS) $(DLLLIBS)
else
ifeq ($(HOSTTYPE_OSTYPE),x86.Solaris)
	CC -G $(LDFLAGS) $(DESTDIR)/LibraryCallbacks${OBJ} -o $(CALLBACKS) $(DLLLIBS)
else
ifeq ($(HOSTTYPE_OSTYPE),intel.Linux)
	g++ --shared $(CFLAGS) $(DESTDIR)/LibraryCallbacks${OBJ} -o $(CALLBACKS) $(DLLLIBS)
else
	cl $(CFLAGS) $(call NATIVEDIR_FUNC,$(DESTDIR)/LibraryCallbacks${OBJ}) /link /DLL /OUT:$(call NATIVEDIR_FUNC,$(CALLBACKS)) $(DLLLIBS)
endif
endif
endif

clean:
	rm -f $(TARGETS) *${OBJ} *.log core $(CALLBACKS)

  
  
