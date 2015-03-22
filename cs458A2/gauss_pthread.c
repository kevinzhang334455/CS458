#ifndef _REENTRANT
#define _REENTRANT		/* basic 3-lines for threads */
#endif

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <math.h>
#include <assert.h>

#define SWAP(a,b)       {double tmp; tmp = a; a = b; b = tmp;}
#define DEBUG


double **matrix, *X, *R;
struct timeval start, finish;

int task_num = 1;
int nsize = 0;

pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

/* Pre-set solution. */

double *X__;

void barrier (int expect)
{
    static int arrived = 0;

    pthread_mutex_lock (&mut);	//lock

    arrived++;
    if (arrived < expect)
        pthread_cond_wait (&cond, &mut);
    else {
        arrived = 0;		// reset the barrier before broadcast is important
        pthread_cond_broadcast (&cond);
    }

    pthread_mutex_unlock (&mut);	//unlock
}


int initMatrix(const char *fname)
{
    FILE *file;
    int l1, l2, l3;
    double d;
    int nsize;
    int i, j;
    double *tmp;
    char buffer[1024];
    char *p1,*p2;
    if ((file = fopen(fname, "r")) == NULL) {
	fprintf(stderr, "The matrix file open error\n");
        exit(-1);
    }
    
    /* Parse the first line to get the matrix size. */
    p1 = fgets(buffer, 1024, file);
    sscanf(buffer, "%d %d %d", &l1, &l2, &l3);
    nsize = l1;
#ifdef DEBUG1
    fprintf(stdout, "matrix size is %d\n", nsize);
#endif

    /* Initialize the space and set all elements to zero. */
    /* set allocations together at here : */          
    matrix = (double**)malloc(nsize*sizeof(double*));
    assert(matrix != NULL);
    tmp = (double*)malloc(nsize*nsize*sizeof(double));
    assert(tmp != NULL);        
    X__ = (double*)malloc(nsize * sizeof(double));
    assert(X__ != NULL);
    R = (double*)malloc(nsize * sizeof(double));
    assert(R != NULL);
    X = (double*)malloc(nsize * sizeof(double));
    assert(X != NULL);
    /******************************************************/
    for (i = 0; i < nsize; i++) {
        matrix[i] = tmp;
        tmp = tmp + nsize;
    }
    for (i = 0; i < nsize; i++) {
        for (j = 0; j < nsize; j++) {
            matrix[i][j] = 0.0;
        }
    }

    /* Parse the rest of the input file to fill the matrix. */
    for (;;) {
	   p2 = fgets(buffer, 1024, file);
	   sscanf(buffer, "%d %d %lf", &l1, &l2, &d);
	   if (l1 == 0) break;
	   matrix[l1-1][l2-1] = d;
    }
    fclose(file);
    return nsize;
}

/* Initialize the right-hand-side following the pre-set solution. */

void initRHS(int nsize, int begin, int end)
{
    int i, j;
    begin = begin-1;
    end = end-1;
    for (i = 0; i < nsize; i++) {
	X__[i] = i+1;
    }
//    barrier (task_num);
    for (i = 0; i < nsize; i++) {
	R[i] = 0.0;
	for (j = 0; j < nsize; j++) {
	    R[i] += matrix[i][j] * X__[j];
	}   
    }
}

/* Initialize the results. */

void initResult(int nsize, int begin, int end)
{
    int i;
    begin = begin-1;
    end = end-1;
    for (i = begin; i <= end; i++) {
	X[i] = 0.0;
    }
    barrier (task_num);
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

int num_iter;

void computeGauss(int nsize, int task_id)
{
    int i, j, k;
    double pivotval;
    int begin, end;

    
    for (i = 0; i < nsize; i++) {
    num_iter = nsize - (i+1);
    begin = (num_iter * task_id) / task_num + 1;
    end = (num_iter * (task_id + 1)) / task_num;
    barrier(task_num);
    if (task_id == 0)
    {
        if (i==9)
           printf("matrix[9][9] = %f, iteration = %d\n", matrix[9][9], i);    
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
     barrier (task_num);
     printf("proc %d: begin = %d, end = %d\n", proc_id, begin ,end);
	/* Factorize the rest of the matrix. */
        for (j = begin + i; j <= end + i; j++) {
            pivotval = matrix[j][i];
            matrix[j][i] = 0.0;
            for (k = i + 1; k < nsize; k++) {
                matrix[j][k] -= pivotval * matrix[i][k];
                if (j==9 && i==8)
                    printf("matrix[9][9] = %f, iteration = %d\n", matrix[9][9], i);    
            }
            R[j] -= pivotval * R[i];
            
        }
     barrier (task_num);
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

void *
work_thread (void *lp)
{
    int task_id = *((int *) lp);
    int begin, end;
    struct timeval start, finish;
    int i;
    /*get the divided task*/
    begin = (nsize * task_id) / task_num + 1;
    end = (nsize * (task_id + 1)) / task_num;
    if(task_id==0)
        gettimeofday (&start, NULL);
    fprintf (stderr, "thread %d: begin %d, end %d\n", task_id, begin, end);

    barrier (task_num);
    /* initialization */
    if (task_id == 0)
    initRHS(nsize, begin, end);
    barrier (task_num);
    initResult(nsize, begin, end);
    barrier (task_num);
    /* Gauss Compute */
    computeGauss(nsize, task_id);
    barrier (task_num);

    /* Gauss computation done */
    if(task_id==0)
    {
	/* Since there are dependencies in computing equation stage(the upper part need the results of upper part), it should be done by thread 0 solely. */
	solveGauss(nsize);
        gettimeofday (&finish, NULL);
        printf ("Elapsed time: %.2f seconds\n",
	        (((finish.tv_sec * 1000000.0) + finish.tv_usec) -
	        ((start.tv_sec * 1000000.0) + start.tv_usec)) / 1000000.0);
     }
	
}

int main (int argc, char *argv[])
{
    int c, i, j;
    int begin, end;
    int iTotalSize;
    pthread_attr_t attr;
    pthread_t *tid;
    int *id;
    double error;
    if (argc == 1)
    {
	printf("This program should take at least one parameter for input matrix\n");
	return 0;
    }
    if (argc == 3)
	task_num = atoi(argv[2]);
    if (argc > 3)
    {
	printf("This program should not take more than 2 parameters \n");
	return 0;
    }
    nsize = initMatrix(argv[1]);
    id = (int *) malloc (sizeof (int) * task_num);
    tid = (pthread_t *) malloc (sizeof (pthread_t) * task_num);
    if (!id || !tid)
    {
	fprintf(stderr, "The matrix file open error\n");
        exit(-1);
    }
    pthread_attr_init (&attr);
    pthread_attr_setscope (&attr, PTHREAD_SCOPE_SYSTEM);
    for (i = 1; i < task_num; i++) {
        id[i] = i;
        pthread_create (&tid[i], &attr, work_thread, &id[i]);
    }
    id[0]=0;
    work_thread(&id[0]);
    // wait for all threads to finish
    for (i = 1; i < task_num; i++)
        pthread_join (tid[i], NULL);

     FILE *result;
     result = fopen ("result.txt", "w");
     for (i=0; i<nsize; i++){
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
     return 0;
}
    
