OBJDIR=$(OSBUILDDIR)/src/pdxautoserializer
ANTLR_SRCS=impl/CPPParser/CPPLexer.cpp impl/CPPParser/CPPParser.cpp
ANTLR_HEADERS=impl/CPPParser/CPPLexer.hpp impl/CPPParser/CPPParser.hpp impl/CPPParser/STDCTokenTypes.hpp impl/CPPParser/STDCTokenTypes.txt
SRCS:=$(wildcard *.cpp) $(wildcard impl/*.cpp) $(wildcard impl/CPPParser/*.cpp) 
OBJS:=$(SRCS:.cpp=$(OBJDIR).o)
INCLUDES:=-I. -I$(ANTLR)/include
DESTDIR=$(OSBUILDDIR)/src/executables

ifeq ($(OSNAME),SunOS)
  UNAME_P := $(shell uname -p)
  CXX = $(SunCompilerDir)/CC
  ifeq ($(GFLIB_MODEL),64bit)
    CXXEXTRA_FLAGS = -m64 -KPIC
  else
    CXXEXTRA_FLAGS = -m32 -KPIC
  endif
  # using -xMMD to fix issue with implicit rule to rebuild string from spring cc
  CXXEXTRA_FLAGS += -xMMD
else
ifeq ($(OSNAME),Linux)
	ifeq ($(GFLIB_MODEL),64bit)
		CXXEXTRA_FLAGS = -m64
	else
		CXXEXTRA_FLAGS = -m32
	endif
	CXXEXTRA_FLAGS += -MD
else
	echo "Not Supported Platform"
endif
endif

FILES_o := $(foreach item, $(subst .cpp,.o,$(SRCS)),$(OBJDIR)/$(item))

.PHONY: directories

all: directories $(DESTDIR)/pdxautoserializer
#all: $(SRCS)
#	echo "Rebuilding Dependencies..."
#	$(CXX) $(CXXEXTRA_FLAGS) $(INCLUDES) $^ -o $(DESTDIR)/pdxautoserializer -L$(ANTLR)/lib -lantlr
#	rm -f *.[oi] impl/*.[oi] impl/CPPParser/*.[oi] 

$(OBJDIR)/%.o: %.cpp
	@echo compiling $(VARIANT) $<
	$(CXX) $(CXXEXTRA_FLAGS) $(INCLUDES) $< -o $@ -c

$(DESTDIR)/pdxautoserializer: $(FILES_o)
	$(CXX) $(CXXEXTRA_FLAGS) -o $@ $(FILES_o) -L$(ANTLR)/lib -lantlr

DIRECTORIES=$(OBJDIR) $(OBJDIR)/impl $(OBJDIR)/impl/CPPParser
directories: $(DIRECTORIES)

$(DIRECTORIES):
	@mkdir -p $@

-include $(FILES_o:.o=.d)
