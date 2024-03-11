use std::{sync::Arc, thread, time::Instant};

use criterion::BenchmarkId;
use criterion::{criterion_group, Criterion};
use criterion_perf_events::Perf;
use perfcnt::linux::HardwareEventType as Hardware;
use perfcnt::linux::PerfCounterBuilderLinux as Builder;
use utils::read_data;
mod utils;

fn bench_no_pinning(c: &mut Criterion<Perf>) {
    let mut group = c.benchmark_group("Independent output");
    let data = read_data("./test.data");
    for num_threads in [1, 2, 4, 8, 16, 32].iter() {
        for num_hash_bits in 0..18 {
            let input = utils::Input {
                num_threads: *num_threads as i32,
                num_hash_bits
            };
            group.bench_with_input(BenchmarkId::from_parameter(num_threads), &input, |b, &input| {
                indepenent_lol(input);
            });
        }
    }
    group.finish();
}

criterion_group!(
    name = independent_no_pin;
    config = Criterion::default().with_measurement(Perf::new(Builder::from_hardware_event(Hardware::CacheMisses)));
    targets = bench_no_pinning
);

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
        let hash = utils::hash(*key as i64, num_hash_bits);
        buffers[hash as usize].push(*payload);
    }
}