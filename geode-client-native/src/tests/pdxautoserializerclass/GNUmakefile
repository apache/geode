.PHONY: all
all: autopdx directories libraries

.PHONY: clean
clean:
	@rm -rf $(SERIALIZERS) $(RELEASE_DIR) $(DEBUG_DIR) $(RELEASE_LIB) $(DEBUG_LIB)

.PHONY: directories
directories:
	@mkdir -p $(RELEASE_DIR) $(DEBUG_DIR)

NAMESPACE = AutoPdxTests
CLASSES = \
	NestedPdx \
	AutoPdxVersioned1 \
	AutoPdxVersioned2 \
	PdxTypes1 \
	PdxTypes2 \
	PortfolioPdx \
	PositionPdx

ifeq ($(strip $(NAMESPACE)),)
	SERIALIZER_PATTERN = %Serializable.cpp
else
	SERIALIZER_PATTERN = $(NAMESPACE)_%Serializable.cpp
endif
SERIALIZERS = $(patsubst %,$(SERIALIZER_PATTERN),$(CLASSES))
AUTOPDXSERIALIZER = $(GFCPP)/bin/pdxautoserializer
AUTOPDXSERIALIZER_FLAGS = 

.PHONY: autopdx
autopdx: $(CLASSES)

.PHONY: $(CLASSES)
$(CLASSES): % : $(SERIALIZER_PATTERN)

$(SERIALIZER_PATTERN): %.hpp
	$(AUTOPDXSERIALIZER) --className=% $(AUTOPDXSERIALIZER_FLAGS) $<

# Multiple classes per file
AutoPdxTests_AutoPdxVersioned1Serializable.cpp : AutoPdxVersioned1.hpp
AutoPdxTests_NestedPdxSerializable.cpp : AutoPdxVersioned1.hpp
AutoPdxTests_PdxTypes1Serializable.cpp : AutoPdxVersioned1.hpp
AutoPdxTests_PdxTypes2Serializable.cpp : AutoPdxVersioned1.hpp

# Use specific class names
AutoPdxTests_AutoPdxVersioned1Serializable.cpp : AUTOPDXSERIALIZER_FLAGS += --classNameStr=AutoPdxVersioned1:PdxTests.PdxVersioned
AutoPdxTests_AutoPdxVersioned2Serializable.cpp : AUTOPDXSERIALIZER_FLAGS += --classNameStr=AutoPdxVersioned2:PdxTests.PdxVersioned




#.SECONDEXPANSION:
.PHONY: libraries
libraries: make-release make-debug

.PHONEY: make-release
make-release:
	$(MAKE) release

.PHONEY: make-debug
make-debug:
	$(MAKE) debug


#
# Project files
#
SRCS = $(wildcard *.cpp)
OBJS = $(SRCS:.cpp=.o)
LIB  = libpdxobject.so

#
# Debug build settings
#
DEBUG_DIR = debug
DEBUG_LIB = $(OSBUILDDIR)/framework/lib/debug/$(LIB)
DEBUG_OBJS = $(addprefix $(DEBUG_DIR)/, $(OBJS))
DEBUG_CXXFLAGS = -g -O0 -DDEBUG=1 -DASSERT_LEVEL=4

#
# Release build settings
#
RELEASE_DIR = release
RELEASE_LIB = $(OSBUILDDIR)/framework/lib/$(LIB)
RELEASE_OBJS = $(addprefix $(RELEASE_DIR)/, $(OBJS))
RELEASE_CFLAGS = -O3 -DNDEBUG
RELEASE_CXXFLAGS = -O3 -DNDEBUG=1 -DASSERT_LEVEL=1

ifeq ($(GFLIB_MODEL),64bit)
  CXXFLAGS = -m64
else
  CXXFLAGS = -m32
endif

CXXFLAGS += -D_REENTRANT

ifeq ($(OSNAME),SunOS)
  UNAME_P := $(shell uname -p)
  CXX = CC
  # using -xMMD to fix issue with implicit rule to rebuild string from spring cc
  CXXFLAGS += -xMMD
  
  CXXFLAGS += -KPIC \
      -mt -D_RWSTD_MULTI_THREAD -DTHREAD=MULTI \
      -D_SOLARIS 
  CXXLDFLAGS += -G -Bdynamic -lpthread


else
ifeq ($(OSNAME),Linux)
  CXXFLAGS += -MD
	
  CXXFLAGS += -fPIC -Wall
  CXXLDFLAGS += -shared
  DEBUG_CXXFLAGS += -fno-inline
else
  $(error "Not Supported Platform")
endif
endif

CXXFLAGS += -I$(GFCPP)/include \
    -I../../tests/fwklib -I../../tests \
    -L$(GFCPP)/lib \
    -lgfcppcache


#
# Debug rules
#
.PHONY: debug
debug: directories $(DEBUG_LIB)

$(DEBUG_LIB): $(DEBUG_OBJS)
	$(CXX) $(CXXFLAGS) $(CXXLDFLAGS) $(DEBUG_CXXFLAGS) -o $(DEBUG_LIB) $^

$(DEBUG_DIR)/%.o: %.cpp
	$(CXX) -c $(CXXFLAGS) $(DEBUG_CXXFLAGS) -o $@ $<

#
# Release rules
#
.PHONY: release
release: directories $(RELEASE_LIB)

$(RELEASE_LIB): $(RELEASE_OBJS)
	$(CXX) $(CXXFLAGS) $(CXXLDFLAGS) $(RELEASE_CXXFLAGS) -o $(RELEASE_LIB) $^

$(RELEASE_DIR)/%.o: %.cpp
	$(CXX) -c $(CXXFLAGS) $(RELEASE_CXXFLAGS) -o $@ $<


-include $(RELEASE_OBJS:.o=.d)
-include $(DEBUG_OBJS:.o=.d)
