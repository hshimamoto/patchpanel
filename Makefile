CC = gcc
CFLAGS = -g -O2 -Wall
LDFLAGS = -g -O2

all: patchpanel

patchpanel: patchpanel.o
	$(CC) $(LDFLAGS) -o $@ patchpanel.o

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f patchpanel *.o
