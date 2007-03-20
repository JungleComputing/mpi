all:
	${MPI}/bin/mpicc -ansi -fPIC -Wall -c -I${MPI}/include -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -I../../lib/include -o ../../lib/mpi.o mpi.c
	# ${LD} -i -o libmpi.o mpi.o ${MPI}/lib/libmpich.a /usr/local/gm/current/lib/libgm.a
	# ${LD} -shared -o ../../lib/libIbisMPIInterface.so libmpi.o
	# ${LD} -shared -o ../../lib/libIbisMPIInterface.so *.o ${MPI}/lib/libmpich.a /usr/local/gm/current/lib/libgm.a
	${LD} -shared -o ../../lib/libIbisMPIInterface.so ../../lib/mpi.o
	rm -f ../../lib/mpi.o


clean:
	rm -f ../../lib/libIbisMPI.so ../../lib/*.o
