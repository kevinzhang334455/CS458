# makefile for digits recognizer
CC = mpicc
CFLAGS = -O3
LFLAGS =

CTAGS =     ctags

CFILES =   gauss_pthread.c gauss_pthread_v2.c
OBJECTS =  gauss_pthread.o gauss_pthread_v2.o
BINS = gauss1 gauss2 

HFILES = 
OTHERSOURCES =  
SOURCES =   $(HFILES) $(CFILES) $(OTHERSOURCES)
all: gauss1 gauss2

gauss1: gauss_mpi.o
	$(CC) -o gauss1 gauss_mpi.o $(LFLAGS)
gauss1.o: gauss_mpi.c
	$(CC) $(CFLAGS) gauss_mpi.c

gauss2: gauss_mpi_v2.o
	$(CC) -o gauss2 gauss_mpi_v2.o $(LFLAGS)
gauss2.o: gauss_pthread_v2.c
	$(CC) $(CFLAGS) gauss_mpi_v2.c

tags: $(CFILES) $(HFILES) $(EXTRACTCFILES) $(RECOGCFILES)
# for vi:
	$(CTAGS) -d -t -T -w $(CFILES) $(HFILES) 
	sort -o tags tags
clean:
	-rm $(BIN) $(OBJECTS) 
