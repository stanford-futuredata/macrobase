
CC=clang++
CFLAGS=-std=c++11 -O3 -march=native -g -Wall
INCLUDES=-I./parquet-cpp/build/debug/include -I./parquet-cpp/src
LDFLAGS=-Lparquet-cpp/build/debug -lparquet -larrow
SRC=main.cpp
TARGET=macrodiff


.PHONY: all clean

all:
	$(CC) $(CFLAGS) $(INCLUDES) $(LDFLAGS) $(SRC) -o $(TARGET)

clean:
	rm -rf $(TARGET)
