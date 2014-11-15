#!/bin/bash

##############################################################################
# If you want to make changes in this script permanent
# you have to copy and paste the lines below
# to the bottom of your ${HOME}/.bashrc  file
##############################################################################

#################           Copy FROM here to .bashrc        #################

if [ ! -d /u/cs458/installed/mpi/3.0.2 ]
then
    # echo "This machine is not supported."
    # echo "You have to run your MPI program in a supported machine."
    # echo "Check http://www.cs.rochester.edu/~kshen/csc258-spring2014"
    # echo "for a list of machines you can use."
    exit 1
fi

ARCH=`uname -m`

MPI_HOME=''
if [ "${ARCH}" == "i686" ]
then
    # echo "Setting up x86 MPI env"
    MPI_HOME=/u/cs458/installed/mpi/3.0.2/x86
elif [ "${ARCH}" == "x86_64" ]
then
    # echo "Setting up x86-64 MPI env"
    MPI_HOME=/u/cs458/installed/mpi/3.0.2/x86_64
else
    # echo "Architecture ${ARCH} not recognized; cannot set up MPI env"
    exit 1;
fi
LD_LIBRARY_PATH=${MPI_HOME}/lib:${LD_LIBRARY_PATH};
PATH=${MPI_HOME}/bin:${PATH}

unset ARCH
unset MPI_HOME
export LD_LIBRARY_PATH
export PATH

#################           Copy  TO  here to .bashrc        #################


##############################################################################
# Note: If you have set-up your environment properly, the following
# line should give you the full set of nodes that will be used when
# you run your mpi program

# mpirun -np ${NumberOfProcesses} -machinefile hosts hostname

##############################################################################
# If you don't want to make permanent changes, you can compile and run 
# your MPI program by calling it INSIDE this script, adding a line below;
# changes in the environment variables above will have no effect outside
# this script. Make sure to compile for and run on machines of the
# same architecture (all x86 or all x86-64).
# Check http://www.cs.rochester.edu/~kshen/csc258-spring2014" for
# information on regarding other system parameter that need to be set up
# (hosts file, .rhosts file)
##############################################################################

# COMPILE
echo "Compiling your MPI program ..."
make clean;
make;
sleep 1;
echo "----------------------------------------------------------------"
# RUN
echo "Running your MPI program ..."
NumberOfProcesses=4
mpirun -np ${NumberOfProcesses} -machinefile hosts linux/gauss_v1 /u/kshen/matrices/original/jpwh_991.dat

