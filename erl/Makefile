SHELL=/bin/sh

EFLAGS=

.PHONY: ebins tgz

all: ebins

tgz:

ebins:
	test -d ebin || mkdir ebin
	erl $(EFLAGS) -make

clean:
	rm -rf ebin
