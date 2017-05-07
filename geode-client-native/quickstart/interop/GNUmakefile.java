default: all

.NOTPARALLEL:

SRC = $(filter-out GNUmakefile.java,$(wildcard *.java))
CLASSES = $(patsubst %.java,%.class,$(SRC))

CLASSPATH += $(GEMFIRE)/lib/gemfire.jar

JAVAC := javac
JAVACFLAGS := -classpath $(CLASSPATH)

.PHONY: all clean
all: $(CLASSES)

clean:
	rm -f $(CLASSES)

%.class : %.java
	$(JAVAC) $(JAVACFLAGS) $(SRC)

