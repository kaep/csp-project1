for part_method in 1 2 3 4;
do 
	for num_threads in 1 2 4 8 16 32 
	do
		for num_bits in {1..18};
		do
			perf stat -o results/$part_method-$num_threads-$num_bits.txt -e cycles,instructions,L1-icache-load-misses,L1-dcache-load-misses,LLC-load-misses,cache-misses,uops_retired.stall_cycles,branch-misses,iTLB-load-misses,dTLB-load-misses cargo +nightly run run $num_threads $num_bits $part_method
		done
	done
done
