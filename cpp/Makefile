
CC=clang++
CFLAGS=-std=c++11 -lstdc++ -march=native
WARNINGS=-Wall -Werror -Wnull-dereference
INCLUDES=
LDFLAGS=-L./sql-parser -lsqlparser -lreadline
SRC=main.cpp
TARGET=macrodiff


.PHONY: all clean

all:
	$(CC) $(CFLAGS) -g -O3 $(WARNINGS) $(INCLUDES) $(LDFLAGS) $(SRC) -o $(TARGET)

debug:
	$(CC) $(CFLAGS) -g -O0 -DDEBUG $(WARNINGS) $(INCLUDES) $(LDFLAGS) $(SRC) -o $(TARGET)

clean:
	rm -rf $(TARGET)
