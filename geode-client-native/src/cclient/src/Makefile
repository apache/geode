CC=gcc
ARCH=32
CFLAGS=-m$(ARCH) -Wall -g3 -fPIC	
INCLUDES=
LFLAGS=-m$(ARCH) -shared
LIBS=
NAME=libgfcclient.so

SRCS=data_io.c gf_client.c
OBJS=data_io.o gf_client.o

.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<  -o $@

.PHONY: clean

all: $(NAME)

$(NAME): $(OBJS) 
	$(CC) -o $(NAME) $(OBJS) $(LFLAGS) $(LIBS)

clean:
	$(RM) *.o *~ $(NAME)


data_io.o: data_io.h
gf_client.o: data_io.h gf_client.h
