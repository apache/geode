SUBDIRS=src sample
ARCH=32
MFLAGS=ARCH=$(ARCH)
FILES=src/gf_client.h src/libgfcclient.so

all: all_subdirs
	mkdir -p dist
	cp $(FILES) dist

clean: clean_subdirs
	rm -fr dist

all_subdirs:
	@for i in $(SUBDIRS); do \
        echo "make all in $$i..."; \
        (cd $$i; $(MAKE) $(MFLAGS) all); done
 
clean_subdirs:
	@for i in $(SUBDIRS); do \
        echo "Clearing in $$i..."; \
        (cd $$i; $(MAKE) $(MFLAGS) clean); done
 
