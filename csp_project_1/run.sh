for part_method in 1 2;
do 
	for num_threads in 1 2 4 8 16 32 
	do
		for num_bits in {1..18};
		do
			cargo r $num_threads $num_bits $part_method
		done
	done
done
