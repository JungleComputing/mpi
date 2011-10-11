all:
	${MPI_HOME}/bin/mpicc -O3 -ansi -fPIC -Wall -c -I${MPI_HOME}/include -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I../../lib/include -o ../../lib/mpi.o mpi.c
	# ${LD} -i -o libmpi.o mpi.o ${MPI_HOME}/lib/libmpich.a /usr/local/gm/current/lib/libgm.a
	# ${LD} -shared -o ../../lib/libIbisMPIInterface.so libmpi.o
	# ${LD} -shared -o ../../lib/libIbisMPIInterface.so *.o ${MPI_HOME}/lib/libmpich.a /usr/local/gm/current/lib/libgm.a
	${LD} -shared -o ../../lib/libIbisMPIInterface.so ../../lib/mpi.o
	rm -f ../../lib/mpi.o


clean:
	rm -f ../../lib/libIbisMPI.so ../../lib/*.o
