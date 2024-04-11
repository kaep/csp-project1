
#![allow(unused)]
#![feature(sync_unsafe_cell)]
use clap::{Parser, Subcommand};
use rand::Rng;
use std::{
    borrow::BorrowMut, cell::{SyncUnsafeCell, UnsafeCell}, fs::{self, File}, io::{self, Write}, sync::{atomic::{AtomicUsize, Ordering::{Relaxed, SeqCst}}, Arc}, thread, time::Instant
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
                // println!(
                //     "#threads {} #bits {} part method{}",
                //     num_threads, num_hash_bits, partitioning_method
                // );
            match partitioning_method {
                1 => {
                    let data = read_data("./2to24.data");
                    independent_output(Arc::new(data), num_threads, num_hash_bits);
                },
                2 => {
                    let data = Arc::new(read_data("./test.data"));
                    let n = data.len() as f32;
                    let buffer_size =  ((n / (i32::pow(2, num_hash_bits as u32) as f32)).ceil() * 1.5).ceil();
                    concurrent_output(data, num_hash_bits, buffer_size as i32, num_threads)
                },
                // pinning 
                3 => { 
                    let data = read_data("./test.data");
                    independent_output_pinning(Arc::new(data), num_threads, num_hash_bits) 
                },
                4 => {
                    let data = Arc::new(read_data("./test.data"));
                    let n = data.len() as f32;
                    let buffer_size =  ((n / (i32::pow(2, num_hash_bits as u32) as f32)).ceil() * 1.5).ceil();
                    concurrent_output_pinning(data, num_hash_bits, buffer_size as i32, num_threads)
                }
                _ => panic!("Invalid partitioning method! Pls give 1, 2, 3 or 4"),
            };
            Ok(())
        }
    }?; Ok(())
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
    part_key % i64::pow(2, hash_bits as u32)
}

fn independent_output_pinning(data: Arc<Vec<(u64, u64)>>, num_threads: i32, num_hash_bits: i32) {
    let start = Instant::now();
    let n = data.len() as i32; 
    let buffer_size: i32 = n / (num_threads * i32::pow(2, num_hash_bits as u32));
    let num_buffers: i32 = i32::pow(2, num_hash_bits as u32);

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
                let thread_pinned_succesfully = core_affinity::set_for_current(cloned_core_ids[thread_number as usize % num_available_cores]);
             
                //pinning was successfull
                if thread_pinned_succesfully {
                    independent_output_thread(cloned_chunks, buffer_size as usize, num_buffers, num_hash_bits, thread_number);
                }
            });
        }
    });
}


// I really dont know if all of this Arc'ing is necessary
// given the change to scoped threads
fn independent_output(data: Arc<Vec<(u64, u64)>>, num_threads: i32, num_hash_bits: i32) {
    let n = data.len() as i32; 
    let buffer_size = (n as f32 / (num_threads * i32::pow(2, num_hash_bits as u32)) as f32).ceil();
    let num_buffers: i32 = i32::pow(2, num_hash_bits as u32);

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
}

fn independent_output_thread(chunk: Arc<Vec<&[(u64, u64)]>>, buffer_size: usize, num_buffers: i32, num_hash_bits: i32, thread_number: i32) {
    let mut buffers: Vec<Vec<(u64, u64)>> = vec![vec![(0, 0); buffer_size]; num_buffers as usize];
    for (key, payload) in chunk[thread_number as usize] {
        let hash = hash(*key as i64, num_hash_bits);
        buffers[hash as usize].push((*key, *payload));
    }
}


fn concurrent_output(data: Arc<Vec<(u64, u64)>>, num_hash_bits: i32, buffer_size: i32, num_threads: i32) {
    //b hash bits gives 2^b output partitions
    let num_partitions = i32::pow(2, num_hash_bits as u32);
    let chunk_size = (data.len() as f32 / num_threads as f32).ceil();
    let cloned = Arc::clone(&data);
    let chunks = Arc::new(cloned.chunks(chunk_size as usize).collect::<Vec<_>>());

    let mut buffers: Vec<(SyncUnsafeCell<Vec<(u64, u64)>>, AtomicUsize)> = Vec::with_capacity(num_partitions as usize);
    // init / allocate all buffers beforehand
    for _ in 0..num_partitions {
         buffers.push((SyncUnsafeCell::new(vec![(0u64, 0u64); buffer_size as usize]), AtomicUsize::new(0)));
    }

    thread::scope(|s| {
        for thread_number in 0..num_threads {
            let cloned_chunks = Arc::clone(&chunks);
            let buffers = &buffers;
            s.spawn(move || {
                //println!("Thread # {} has chunk of size {}", thread_number, cloned_chunks[thread_number as usize].len());
                for (key, payload) in cloned_chunks[thread_number as usize] {
                    let hash = hash(*key as i64, num_hash_bits);
                    let (vec, counter) = &buffers[hash as usize];
                    let index = counter.fetch_add(1, SeqCst);
                    unsafe {
                        *(*vec.get()).get_unchecked_mut(index) = (*key, *payload);
                    }                                                                                 
                }
            });
        }
    });
    //validate_output(data.len(), &buffers);
}


fn concurrent_output_pinning(data: Arc<Vec<(u64, u64)>>, num_hash_bits: i32, buffer_size: i32, num_threads: i32) {
    //b hash bits gives 2^b output partitions
    let num_partitions = i32::pow(2, num_hash_bits as u32);
    let chunk_size = (data.len() as f32 / num_threads as f32).ceil();
    let cloned = Arc::clone(&data);
    let chunks = Arc::new(cloned.chunks(chunk_size as usize).collect::<Vec<_>>());

    let mut buffers: Vec<(SyncUnsafeCell<Vec<(u64, u64)>>, AtomicUsize)> = Vec::with_capacity(num_partitions as usize);
    // init / allocate all buffers beforehand
    for _ in 0..num_partitions {
         buffers.push((SyncUnsafeCell::new(vec![(0u64, 0u64); buffer_size as usize]), AtomicUsize::new(0)));
    }

    let core_ids = Arc::new(core_affinity::get_core_ids().unwrap());
    let num_available_cores = core_ids.len();

    thread::scope(|s| {
        for thread_number in 0..num_threads {
            let cloned_chunks = Arc::clone(&chunks);
            let cloned_core_ids = core_ids.clone();
            let buffers = &buffers;
            s.spawn(move || {
                let thread_number = thread_number;
                let thread_pinned_succesfully = core_affinity::set_for_current(cloned_core_ids[thread_number as usize % num_available_cores]);

                if thread_pinned_succesfully {
                    for (key, payload) in cloned_chunks[thread_number as usize] {
                        let hash = hash(*key as i64, num_hash_bits);
                        let (vec, counter) = &buffers[hash as usize];
                        let index = counter.fetch_add(1, SeqCst);
                        unsafe {
                            *(*vec.get()).get_unchecked_mut(index) = (*key, *payload);
                        }                                                                                 
                    }
                }
            });
        }
    });
    //validate_output(data.len(), &buffers);
}

fn validate_output(data_size: usize, buffers: &Vec<(SyncUnsafeCell<Vec<(u64, u64)>>, AtomicUsize)>) {
    println!("There are {} elements in total", data_size);
    let mut counter = 0;
    let mut total_counter = 0;
    for (buffer, _) in buffers {
        let mut buffer_counter = 0;
        for (key, _) in unsafe { (&*buffer.get()) } {
            if *key != 0 {
                buffer_counter += 1;
            }
        }

        println!("Buffer # {} has {} elements", counter, buffer_counter ); // first might be
        // off-by-one? yes :)
        counter += 1;
        total_counter += buffer_counter;
    }
    println!("Total counter: {} - should be off-by-one compared to total amount of elements", total_counter);
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
