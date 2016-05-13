CC=gcc
ARCH=32
CFLAGS=-m$(ARCH) -Wall -g3
INCLUDES=-I../src
LFLAGS=-m$(ARCH) -L../src
LIBS=-lgfcclient
NAME=sample

SRCS=sample.c
OBJS=sample.o

.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<  -o $@

.PHONY: clean

all: $(NAME)

$(NAME): $(OBJS) 
	$(CC) -o $(NAME) $(OBJS) $(LFLAGS) $(LIBS)

clean:
	$(RM) *.o *~ $(NAME)

sample.o: ../src/gf_client.h
