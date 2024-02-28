use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let num_threads = args[1].parse::<i32>().expect("cmd args parsing");
    println!("Amount of threads {}", num_threads);
}
