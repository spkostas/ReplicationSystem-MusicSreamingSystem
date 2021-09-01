make: mpitest.c hostfile numservers10_np14.txt
	mpicc mpitest.c -o main
	mpirun -np 14 --hostfile hostfile main 10 numservers10_np14.txt
