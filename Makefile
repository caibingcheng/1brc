CC = g++
CFLAGS = -Wall -O3 --std=c++17
CLIBS = -pthread

all:
	$(CC) main.cc -o main $(CFLAGS) $(CLIBS)

clean:
	rm -f main

.PHONY: all clean test
