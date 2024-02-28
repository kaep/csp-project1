use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        println!("Pls give args");
    }
    let num_threads = args[1].parse::<i32>().expect("parsing num threads failed");
    let num_hash_bits = args[2].parse::<i32>().expect("parsing hash bits failed");
    let partitioning_method = args[3].parse::<i32>().expect("parsing partition method failed");

    match partitioning_method {
        1 => independent_output(num_threads, num_hash_bits),
        2 => count_then_move(num_threads, num_hash_bits),
        _ => panic!("Invalid partitioning method! Pls give 1 or 2")
    }


    println!("#threads {} #bits {} #part method{}", num_threads, num_hash_bits, partitioning_method);


    //DATA GENERATION
    //we need to do it ourselves
    //and do it in a way where reading from disk
    //is NOT included in our measurements e.g. 
    //generate just before experiement or make sure 
    //that it is read into memory (not streamed) just before

}

fn hash(part_key: i64, hash_bits: i64) -> i64 {
    //partitioning key is 8 byte aka 64 bits
    part_key % 2 << hash_bits
}

fn independent_output(num_threads: i32, num_hash_bits: i32) {
    //coordination of input tuples to each thread is necessary
    //suggestion: divide input in num_threads blocks and assign
    
    //create output buffers for each thread, for each partition
    //t*(2^b) output buffers
    //where b is the number of hash bits
    //do hash key % hash bits
    //where hash bits is 1-18
}

fn count_then_move(num_threads: i32, num_hash_bits: i32) {

}