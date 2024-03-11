
#![allow(unused)]
#![feature(sync_unsafe_cell)]
use clap::{Parser, Subcommand};
use rand::Rng;
use std::{
    borrow::BorrowMut, cell::{SyncUnsafeCell, UnsafeCell}, fs::{self, File}, io::{self, Write}, sync::{atomic::{AtomicUsize, Ordering::Relaxed}, Arc}, thread, time::Instant
};
use std::time;
use std::sync::atomic::AtomicU64;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(arg_required_else_help = true)]
    Gen { size: usize, file: String },

    #[command(arg_required_else_help = true)]
    Run {
        num_threads: i32,
        num_hash_bits: i32,
        partitioning_method: i32,
    },
}

fn main() -> io::Result<()> {
    let args = Cli::parse();
    match args.command {
        Commands::Gen { size, file } => gen_data(size, file.as_str()),
        Commands::Run {
            num_threads,
            num_hash_bits,
            partitioning_method,
        } => {
                //println!("How many logical cores? {}", core_affinity::get_core_ids().unwrap().len());
                println!(
                    "#threads {} #bits {} part method{}",
                    num_threads, num_hash_bits, partitioning_method
                );
            match partitioning_method {
                1 => {
                    let data = read_data("./test.data");
                    independent_output(Arc::new(data), num_threads, num_hash_bits);
                },
                2 => {
                    let data = Arc::new(read_data("./test.data"));
                    let n = data.len() as f32;
                    let buffer_size = (n / (num_threads * i32::pow(2, num_hash_bits as u32)) as f32).ceil() * 1.5;
                    concurrent_output(data, num_hash_bits, buffer_size as i32, num_threads)
                },
                _ => panic!("Invalid partitioning method! Pls give 1 or 2"),
            };
            Ok(())
        }
    }?;
    Ok(())
}

fn read_data(file_path: &str) -> Vec<(u64, u64)> {
    //file consisting of tuples of 8 byte partitioning key and 8 byte payload
    //could have used byteorder crate
    //but resolved to an answer by Alice Ryhl here: https://users.rust-lang.org/t/reading-binary-files-a-trivial-program-not-so-trivial-for-me/56166/3 
    let mut tuples: Vec<(u64, u64)> = Vec::new();
    let file = fs::read(file_path).unwrap();
    for bytes in file.chunks_exact(16) {
        let key =  u64::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]);
        let payload = u64::from_ne_bytes([bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]]);
        //println!("{:x} {:x}", key, payload);
        tuples.push((key, payload));
    }
    tuples
}

fn hash(part_key: i64, hash_bits: i32) -> i64 {
    //partitioning key is 8 byte aka 64 bits
    //part_key % (2 << hash_bits) //TODO: is this correct?
    part_key % i64::pow(2, hash_bits as u32)
}


fn pinning_example(num_threads: i32) {
    let core_ids = Arc::new(core_affinity::get_core_ids().unwrap());
    let num_available_cores = core_ids.len();
    thread::scope(|scope| {
        for thread_number in 0..num_threads {
            let cloned_core_ids = core_ids.clone();
            scope.spawn(move || {
                let thread_number = thread_number;
                //evenly distribute on all available cores
                let res = core_affinity::set_for_current(cloned_core_ids[thread_number as usize % num_available_cores]);
             
                //pinning was successfull
                if res {
                }
            });
        }
    });
}

fn independent_output_pinning(data: Arc<Vec<(u64, u64)>>, num_threads: i32, num_hash_bits: i32) {
    let start = Instant::now();
    println!("Running independent output with pinning on data cardinality {} with {} threads and {} hash bits", data.len(), num_threads, num_hash_bits);
    let n = data.len() as i32; 
    let buffer_size: i32 = n / (num_threads * (2 << num_hash_bits));
    let num_buffers: i32 = num_threads * (2 << num_hash_bits);

    // we need to account for non-divisible data sizes somehow?
    // maybe see PCPP code
    let chunk_size = (data.len() as f32 / num_threads as f32).ceil();
    
    let cloned = Arc::clone(&data);
    let chunks = Arc::new(cloned.chunks(chunk_size as usize).collect::<Vec<_>>());

    let core_ids = Arc::new(core_affinity::get_core_ids().unwrap());
    let num_available_cores = core_ids.len();
    thread::scope(|scope| {
        for thread_number in 0..num_threads {
            let cloned_core_ids = core_ids.clone();
            let cloned_chunks = Arc::clone(&chunks);
            scope.spawn(move || {
                let thread_number = thread_number;
                //evenly distribute on all available cores
                let res = core_affinity::set_for_current(cloned_core_ids[thread_number as usize % num_available_cores]);
             
                //pinning was successfull
                if res {
                    independent_output_thread(cloned_chunks, buffer_size as usize, num_buffers, num_hash_bits, thread_number);
                }
            });
        }
    });

}


// I really dont know if all of this Arc'ing is necessary
// given the change to scoped threads
fn independent_output(data: Arc<Vec<(u64, u64)>>, num_threads: i32, num_hash_bits: i32) {
    let start = Instant::now();
    println!("Running independent output on data cardinality {} with {} threads and {} hash bits", data.len(), num_threads, num_hash_bits);
    let n = data.len() as i32; 
    let buffer_size = (n as f32 / (num_threads * (2 << num_hash_bits)) as f32).ceil();
    println!("Buffer size {}", buffer_size);
    let num_buffers: i32 = num_threads * (2 << num_hash_bits);

    // we need to account for non-divisible data sizes somehow?
    // maybe see PCPP code
    let chunk_size = (data.len() as f32 / num_threads as f32).ceil();
    
    let cloned = Arc::clone(&data);
    let chunks = Arc::new(cloned.chunks(chunk_size as usize).collect::<Vec<_>>());

    thread::scope(|s| {
        for thread_number in 0..num_threads {
            let cloned_chunks = Arc::clone(&chunks);
            s.spawn(move || {
                independent_output_thread(cloned_chunks, buffer_size as usize, num_buffers, num_hash_bits, thread_number);
            });
        }
    });
    let elapsed_time = start.elapsed();
    println!("Independent output processed {} tuples in {} seconds", data.len(), elapsed_time.as_secs_f64());
}

fn independent_output_thread(chunk: Arc<Vec<&[(u64, u64)]>>, buffer_size: usize, num_buffers: i32, num_hash_bits: i32, thread_number: i32) {
    let mut buffers: Vec<Vec<u64>> = vec![vec![0; buffer_size]; num_buffers as usize];
    for (key, payload) in chunk[thread_number as usize] {
        let hash = hash(*key as i64, num_hash_bits);
        //println!("Thread {} hashed key {} into {}", thread_number, key, hash);
        
        //TODO: we need to allocate vec beforehand!
        buffers[hash as usize].push(*payload);
    }
}

// this is still chunking right?
// yes: only output needs sync
// but how to write to output?
// seems like the paper uses a single contigous buffer
// but how do we know where the "lines" between partitions go then?
// aah no, still need 2^b buffers aka one for each output partition
// counter is for each of the buffers..
// we need to init all of the vectors beforehand, as we wont be able to index
// into them without
fn concurrent_output(data: Arc<Vec<(u64, u64)>>, num_hash_bits: i32, buffer_size: i32, num_threads: i32) {
    //b hash bits gives 2^b output partitions
    let num_partitions = i32::pow(2, num_hash_bits as u32);
    let chunk_size = (data.len() as f32 / num_threads as f32).ceil();
    let cloned = Arc::clone(&data);
    let chunks = Arc::new(cloned.chunks(chunk_size as usize).collect::<Vec<_>>());

    //let buffers: Vec<(UnsafeCell<Vec<(u64, u64)>>, AtomicU64)>
    // atomic u64 atm. consider whether size can be decreased  -> just usize
    // we need as many counters as there are buffers though!
    // a possible way to circumvent arc in counter is: Vec<UnsafeCell<Vec<tuple>>, AtomicCounter>
    // buffer should be shareable.. unsafecell?

    //mut could be dropped but the vec! macro is complaining about something clone-related, so lets just do it this way
    let mut buffers: Vec<(SyncUnsafeCell<Vec<(u64, u64)>>, AtomicUsize)> = Vec::with_capacity(num_partitions as usize);
    //let mut buffers: Vec<(SyncUnsafeCell<Vec<(u64, u64)>>, AtomicUsize)> = vec![(SyncUnsafeCell::new(vec![(0u64, 0u64); buffer_size as usize]), AtomicUsize::new(0)); num_partitions as usize];

    // init / allocate all buffers beforehand
    for _ in 0..num_partitions {
         buffers.push((SyncUnsafeCell::new(vec![(0u64, 0u64); buffer_size as usize]), AtomicUsize::new(0)));
    }

    println!("Concurrent output buffer size {}", buffer_size);

    thread::scope(|s| {
        for thread_number in 0..num_threads {
            let cloned_chunks = Arc::clone(&chunks);
            //this variable shadowing/aliasing stuff is necessary to explicitly tell the compiler that the thread is getting an 
            // immmutable reference
            // both the inner vec and counter are still mutable though (with some unsafe)
            let buffers = &buffers;
            s.spawn(move || {
                //concurrent_output_thread(cloned_chunks, buffers, thread_number, num_hash_bits);
                //atomic_counter.fetch_add(1, Relaxed);
                for (key, payload) in cloned_chunks[thread_number as usize] {
                    let hash = hash(*key as i64, num_hash_bits);
                    //println!("Hashed {} into {}", *key, hash);

                    // hash is index into buffers
                    
                    let counter = buffers[hash as usize].1.load(Relaxed); 
                    let (vec, counter) = &buffers[hash as usize];
                    unsafe {
                        if counter.as_ptr() as usize > (*vec.get()).len() {
                            //println!("something wong, counter is {:?}", *(counter.as_ptr()));
                        }
                        //println!("Counter is {:?}", counter);
                        //println!("{}", (*vec.get()).len());
                        *(*vec.get()).get_unchecked_mut(*(counter.as_ptr())) = (*key, *payload);
                    }
                    buffers[hash as usize].1.fetch_add(1, Relaxed);
                }
            });
        }
    });
    //println!("concurrent output finished with value of counter: {}", atomic_counter.load(Relaxed));
}

fn concurrent_output_thread(chunk: Arc<Vec<&[(u64, u64)]>>, output: Vec<(Vec<(u64, u64)>, AtomicUsize)>, thread_number: i32, num_hash_bits: i32) {

    for (key, payload) in chunk[thread_number as usize] {
        let hash = hash(*key as i64, num_hash_bits);
    }

}


fn gen_data(size: usize, file: &str) -> io::Result<()> {
    println!("Writing {} tuples to {}...", size, file);
    let mut rng = rand::thread_rng();
    let mut f = File::create(file)?;
    for i in 0..size {
        if i % 1000 == 0 {
            print!("Tuple: {} of {}            \r", i, size)
        }
        let key: u64 = i as u64;
        let val: u64 = rng.gen();
        f.write_all(&key.to_ne_bytes())?;
        f.write_all(&val.to_ne_bytes())?;
    }
    println!("Tuple: {} of {}           ", size, size);
    println!("Done!");

    Ok(())
}
