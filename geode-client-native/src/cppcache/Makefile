CXXFILES    := $(wildcard *.cpp) $(wildcard impl/*.cpp) $(wildcard statistics/*.cpp)
CXXOBJECTS  := $(CXXFILES:.cpp=.o)
CXXHEADERS  := $(wildcard *.hpp) $(wildcard impl/*.hpp) $(wildcard statistics/*.hpp) $(wildcard *.inl) $(wildcard impl/*.inl) $(wildcard statistics/*.inl)

BUILD_DIR_NAME	:= build
CXX      := ${INTEL_HOME}/bin/icpc
CXXFLAGS := -ggdb -D_REENTRANT -m64 -O${OPTIMIZE_LEVEL} -I$(CURDIR) -I${ACE_THIRD_PARTY_HOME} -I${LIBXML_INCLUDE_DIR} -DBUILD_CPPCACHE -std=c++11 -fPIC \
            -D__ACE_INLINE__ -DACE_HAS_LINUX_NPTL -DACE_HAS_AIO_CALLS -DACE_HAS_EXCEPTIONS -DACE_HAS_IPV6 -DACE_HAS_STRDUP_EMULATION
 

LD := $(CXX)
MV := mv
MKDIR := mkdir -p

LDFLAGS := -ggdb -L${ACE_THIRD_PARTY_HOME}/lib/Linux/ -L${INTEL_HOME}/lib/intel64 -L${LIBXML_BIN_DIR} -lgcc_s -lACE -limf -lsvml -lintlc -lirng -lirc -lcilkrts -lxml2

LIB_NAME = gfcppcache

%.o : %.cpp $(CXXHEADERS)
	$(COMPILE.cpp) $(OUTPUT_OPTION) $<

$(BUILD_DIR_NAME)/lib$(LIB_NAME).so : $(CXXOBJECTS)
	$(MKDIR) $(BUILD_DIR_NAME)
	$(MKDIR) impl/$(BUILD_DIR_NAME)
	$(MKDIR) statistics/$(BUILD_DIR_NAME)	
	$(LD) -shared $(LDFLAGS) -o $@ $^
	$(MV) *.o $(BUILD_DIR_NAME)
	$(MV) impl/*.o impl/$(BUILD_DIR_NAME)
	$(MV) statistics/*.o statistics/$(BUILD_DIR_NAME)

clean:
	rm -rf $(BUILD_DIR_NAME) impl/$(BUILD_DIR_NAME) statistics/$(BUILD_DIR_NAME)

