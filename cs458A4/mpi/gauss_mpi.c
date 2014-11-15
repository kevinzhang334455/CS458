#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <math.h>
#include <mpi.h>
#include <assert.h>
#define SWAP(a,b)       {double tmp; tmp = a; a = b; b = tmp;}
#define MAXPROCS 28

double **matrix, *X, *R;
struct timeval start, finish;

int proc_id, nprocs, mytid, tid[MAXPROCS];

int nsize;
/* Pre-set solution. */

double *X__;


int initMatrix(const char *fname)
{
    FILE *file;
    int l1, l2, l3;
    double d;
    int i, j, l;
    double *tmp;
    char buffer[1024];
    char *p1,*p2;
    int begin, end;
    int nsize0;

    if (proc_id == 0) {
      if ((file = fopen(fname, "r")) == NULL) {
        fprintf(stderr, "The matrix file open error\n");
        exit(-1);
      }
    /* Parse the first line to get the matrix size. */
      p1 = fgets(buffer, 1024, file);
      sscanf(buffer, "%d %d %d", &l1, &l2, &l3);
      nsize0 = l1;
    }

    MPI_Bcast(&nsize0, 1, MPI_INT, 0, MPI_COMM_WORLD);

    begin = (nsize0 * proc_id) / nprocs + 1;
    end = (nsize0 * (proc_id + 1)) / nprocs;
    
    /* Initialize the space and set all elements to zero. */
    /* set allocations together at here : */
    matrix = (double**)malloc(nsize0*sizeof(double*));
    assert(matrix != NULL);
    tmp = (double*)malloc(nsize0*nsize0*sizeof(double));
    assert(tmp != NULL);        
    R = (double*)malloc(nsize0 * sizeof(double));
    assert(R != NULL);
    if (proc_id == 0) {          
    X__ = (double*)malloc(nsize0 * sizeof(double));
    assert(X__ != NULL);
    X = (double*)malloc(nsize0 * sizeof(double));
    assert(X != NULL);
    }

    /******************************************************/
 
      for (i = 0; i<nsize0; i++) {
          matrix[i] = tmp;
          tmp = tmp + nsize0;
      }
   
      for (i = 0; i < nsize0; i++) {
         for (j = 0; j < nsize0; j++) {
            matrix[i][j] = 0.0;
         }
       }

//      MPI_Barrier(MPI_COMM_WORLD);

      if (proc_id == 0) {
    /* Parse the rest of the input file to fill the matrix. */
        for (;;) {
	    p2 = fgets(buffer, 1024, file);
	    sscanf(buffer, "%d %d %lf", &l1, &l2, &d);
	    if (l1 == 0) break;
 	    matrix[l1-1][l2-1] = d;
        }
      }
      
      MPI_Bcast(&matrix[0][0], nsize0*nsize0, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    if (proc_id == 0)
       fclose(file);
    return nsize0;
}

/* Initialize the right-hand-side following the pre-set solution. */

void initRHS( int nsize )
{
    int i, j, l;
    int begin, end, length;
    if (proc_id == 0) {
      for (i = 0; i < nsize; i++) {
	  X__[i] = i+1;
      }
      for (i = 0; i < nsize; i++) {
	  R[i] = 0.0;
	  for (j = 0; j < nsize; j++) {
	      R[i] += matrix[i][j] * X__[j];
	  }   
      }
    
       for (i = 0; i < nsize; i++) {
           X[i] = 0.0;
     }
   }

   MPI_Bcast(&R[0], nsize, MPI_DOUBLE, 0, MPI_COMM_WORLD);
}

/* Get the pivot - the element on column with largest absolute value. */
/* At here, noticing that getting pivot is a column-based scale, so we can do this in parallel at the working thread function */

void getPivot(int nsize, int currow)
{
    int i, pivotrow;

    pivotrow = currow;
    for (i = currow+1; i < nsize; i++) {
	if (fabs(matrix[i][currow]) > fabs(matrix[pivotrow][currow])) {
	    pivotrow = i;
	}
    }

    if (fabs(matrix[pivotrow][currow]) == 0.0) {
        fprintf(stderr, "The matrix is singular\n");
        exit(-1);
    }
    
    if (pivotrow != currow) {
#ifdef DEBUG1
	fprintf(stdout, "pivot row at step %5d is %5d\n", currow, pivotrow);
#endif
        for (i = currow; i < nsize; i++) {
            SWAP(matrix[pivotrow][i],matrix[currow][i]);
        }
        SWAP(R[pivotrow],R[currow]);
    }
}

/**************************************************************************/
/* In computeGauss, first we get the pivotal of i, make the first element equals to 1.
   The elements in the same row could not be affacted, so we can parallelize it */


void computeGauss(int nsize, int task_id)
{
    int i, j, k, l;
    double pivotval;
    int begin, end;
    int begin1, end1;
    int num_iter;
    begin1 = (nsize * proc_id) / nprocs + 1;
    end1 = (nsize * (proc_id+1)) / nprocs;

    for (i = 0; i < nsize; i++) {
    num_iter = nsize - (i+1);
    begin = (num_iter * proc_id) / nprocs + 1;
    end = (num_iter * (proc_id + 1)) / nprocs;
//   MPI_Barrier(MPI_COMM_WORLD);

    if (proc_id == 0)
    { 
	getPivot(nsize,i);        
	/* Scale the main row. */
        pivotval = matrix[i][i];
	if (pivotval != 1.0) {
	    matrix[i][i] = 1.0;
	    for (j = i + 1; j < nsize; j++) {
		matrix[i][j] /= pivotval;
	    }
	    R[i] /= pivotval;
	}
    }
 
//    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Bcast(&matrix[i][i], nsize-i, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    MPI_Bcast(&R[i], 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
     


	/* Factorize the rest of the matrix. */
    for (j = i+1; j < nsize; j++) {
//    MPI_Barrier(MPI_COMM_WORLD);
      if ( j >= begin+i && j <= end+i ) {
        pivotval = matrix[j][i];
        matrix[j][i] = 0.0;
	for (k = i + 1; k < nsize; k++) {
             matrix[j][k] -= pivotval * matrix[i][k];
           }
        R[j] -= pivotval * R[i];
      }
//      MPI_Barrier(MPI_COMM_WORLD);
      for (l = 0; l< nprocs; l++) {
          begin1 = (num_iter * l)/ nprocs + 1;
          end1= (num_iter * (l+1))/ nprocs;
          if ( j >= begin1+i && j <= end1+i) {
             MPI_Bcast(&matrix[j][i], nsize-i, MPI_DOUBLE, l, MPI_COMM_WORLD);
             MPI_Bcast(&R[j], 1, MPI_DOUBLE, l, MPI_COMM_WORLD);
          }
      }
   }
  }
}


/* Solve the equation. */

void solveGauss(int nsize)
{
    int i, j;

    X[nsize-1] = R[nsize-1];
    for (i = nsize - 2; i >= 0; i --) {
        X[i] = R[i];
        for (j = nsize - 1; j > i; j--) {
            X[i] -= matrix[i][j] * X[j];
        }
    }

#ifdef DEBUG1
    fprintf(stdout, "X = [");
    for (i = 0; i < nsize; i++) {
        fprintf(stdout, "%.6f ", X[i]);
    }
    fprintf(stdout, "];\n");
#endif
}

/************************************************************************/


int main (int argc, char *argv[])
{
    int c, i, j;
    int begin, end;
    int iTotalSize;
    int nsize;
    double error;
    struct timeval start, finish;

    if (argc != 2)
    {
	printf("This program should take and only take one parameter for input matrix\n");
	return 0;
    }

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &proc_id);

    nsize = initMatrix(argv[1]);
 
    /*  begin working... */
    if (proc_id == 0) 
        gettimeofday (&start, NULL);
    initRHS(nsize);
//    MPI_Barrier(MPI_COMM_WORLD);
    computeGauss(nsize, proc_id);
    MPI_Barrier(MPI_COMM_WORLD);
    if (proc_id == 0) {
        solveGauss(nsize);
 	gettimeofday (&finish, NULL);
        printf ("Elapsed time: %.2f seconds\n",
	        (((finish.tv_sec * 1000000.0) + finish.tv_usec) -
	        ((start.tv_sec * 1000000.0) + start.tv_usec)) / 1000000.0); 

       FILE *result;
       result = fopen ("result.txt", "w");
       for (i=0; i<nsize; i++) {
         fprintf (result, "X[%d] = %f \n", i, X[i]);
       }
      error = 0.0;

      for (i = 0; i < nsize; i++) {
 
	  double error__ = (X__[i]==0.0) ? 1.0 : fabs((X[i]-X__[i])/X__[i]);
	  if (error < error__) {
	      error = error__;
	  }
      }
    fprintf(stdout, "Error: %6.3f\n", error);
    }

    MPI_Finalize();
    exit(0);
}
