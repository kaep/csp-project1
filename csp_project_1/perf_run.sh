num_threads=2
num_bits=18
part_method=1
perf stat -e cycles,instructions,iTLB-load-misses,dTLB-load-misses cargo r run $num_threads $num_bits $part_method
