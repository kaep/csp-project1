use criterion::criterion_main;
use criterion::{criterion_group, BenchmarkId, Criterion};
use criterion_perf_events::Perf;
use perfcnt::linux::HardwareEventType as Hardware;
use perfcnt::linux::PerfCounterBuilderLinux as Builder;


#[derive(Clone, Copy)]
struct Input {
    num_threads: i32,
    num_hash_bits: i32
}

fn bench_test(c: &mut Criterion<Perf>) {
    let mut group = c.benchmark_group("Independent output");
    for num_threads in [1, 2, 4, 8, 16, 32].iter() {
        for num_hash_bits in 0..18 {
            let input = Input {
                num_threads: *num_threads as i32,
                num_hash_bits
            };
            group.bench_with_input(BenchmarkId::from_parameter(num_threads*num_hash_bits), &input, |b, &input| {
                b.iter(|| indepenent_lol(input));
            });
        }
    }
    group.finish();
}
fn indepenent_lol(input: Input) {
    println!("{} {}", input.num_threads, input.num_hash_bits);
}
criterion_group!(
    name = benches;
    config = Criterion::default().with_measurement(Perf::new(Builder::from_hardware_event(Hardware::CacheMisses)));
    targets = bench_test
);
criterion_main!(benches);