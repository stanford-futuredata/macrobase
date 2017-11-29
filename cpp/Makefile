
CC=clang++
CFLAGS=-std=c++11 -lstdc++ -march=native
WARNINGS=-Wall -Werror -Wnull-dereference
INCLUDES=
LDFLAGS=-L./sql-parser -lsqlparser -lreadline -lroaring
SRC=main.cpp
TARGET=macrodiff

ifeq ($(OUTLIER_BY_COL), 1)
    CFLAGS += -DOUTLIER_BY_COL
else
    CFLAGS += -DOUTLIER_BY_ROW
endif

ifeq ($(INLIER_BY_COL), 1)
    CFLAGS += -DINLIER_BY_COL
else
    CFLAGS += -DINLIER_BY_ROW
endif


.PHONY: all clean

all:
	$(CC) $(CFLAGS) -g -O3 $(WARNINGS) $(INCLUDES) $(LDFLAGS) $(SRC) -o $(TARGET)

debug:
	$(CC) $(CFLAGS) -g -O0 -DDEBUG $(WARNINGS) $(INCLUDES) $(LDFLAGS) $(SRC) -o $(TARGET)

clean:
	rm -rf $(TARGET)
