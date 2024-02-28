use std::collections::hash_map::DefaultHasher;
use std::{env, hash};
use std::hash::{Hash, Hasher};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Pls give amount of threads as arg");
    }
    let num_threads = args[1].parse::<i32>().expect("cmd args parsing");
    println!("Amount of threads {}", num_threads);

    let max_hash_bits = 18;

    //DATA GENERATION
    //we need to do it ourselves
    //and do it in a way where reading from disk
    //is NOT included in our measurements e.g. 
    //generate just before experiement or make sure 
    //that it is read into memory (not streamed) just before

}


fn independent_output() {
    //coordination of input tuples to each thread is necessary
    //suggestion: divide input in num_threads blocks and assign
    
    //create output buffers for each thread, for each partition
    //t*(2^b) output buffers
    //where b is the number of hash bits
    //do hash key % hash bits
    //where hash bits is 1-18
    

}