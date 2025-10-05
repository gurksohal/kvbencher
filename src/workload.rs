pub mod read_heavy;
pub mod read_only;
pub mod read_write;

use crate::database::Database;
use crate::generator::{ByteGen, KVSizeGen};
use anyhow::Result;
use hdrhistogram::Histogram;
use rand::prelude::SmallRng;
use rand::{Rng, RngCore, SeedableRng, random};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thousands::Separable;

#[derive(Debug)]
pub struct WorkloadStats {
    load_time: Duration,
    load_ops: u64,
    run_wall_time: Duration,
    run_read_time: Duration,
    run_read_ops: u64,
    run_read_hist_micro_sec: Histogram<u64>,
    run_write_time: Duration,
    run_write_ops: u64,
    run_write_hist_micro_sec: Histogram<u64>,
}

impl WorkloadStats {
    pub fn new() -> Result<Self> {
        Ok(WorkloadStats {
            load_time: Duration::ZERO,
            load_ops: 0,
            run_wall_time: Duration::ZERO,
            run_read_time: Duration::ZERO,
            run_read_ops: 0,
            run_read_hist_micro_sec: Histogram::new_with_bounds(1, 10_000_000, 3)?,
            run_write_time: Duration::ZERO,
            run_write_ops: 0,
            run_write_hist_micro_sec: Histogram::new_with_bounds(1, 10_000_000, 3)?,
        })
    }
}

impl Display for WorkloadStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let throughput = |ops: u64, d: Duration| -> f64 {
            if ops == 0 || d.is_zero() {
                0.0
            } else {
                ops as f64 / d.as_secs_f64()
            }
        };
        let percentile = |h: &Histogram<u64>, q: f64| -> String {
            if h.is_empty() {
                "-".into()
            } else {
                h.value_at_quantile(q).separate_with_underscores()
            }
        };

        // reads
        let r_p50 = percentile(&self.run_read_hist_micro_sec, 0.50);
        let r_p95 = percentile(&self.run_read_hist_micro_sec, 0.95);
        let r_p99 = percentile(&self.run_read_hist_micro_sec, 0.99);
        let r_p999 = percentile(&self.run_read_hist_micro_sec, 0.999);

        // writes
        let w_p50 = percentile(&self.run_write_hist_micro_sec, 0.50);
        let w_p95 = percentile(&self.run_write_hist_micro_sec, 0.95);
        let w_p99 = percentile(&self.run_write_hist_micro_sec, 0.99);
        let w_p999 = percentile(&self.run_write_hist_micro_sec, 0.999);

        writeln!(f, "=== LOAD ===")?;
        writeln!(
            f,
            "ops: {} | time: {:.1?} | throughput: {} ops/s",
            self.load_ops.separate_with_underscores(),
            self.load_time,
            (throughput(self.load_ops, self.load_time) as u64).separate_with_underscores()
        )?;

        writeln!(f, "=== RUN READ ===")?;
        writeln!(
            f,
            "ops: {} | time: {:.1?} | throughput: {} ops/s | p50: {} µs | p95: {} µs | p99: {} µs | p99.9: {} µs",
            self.run_read_ops.separate_with_underscores(),
            self.run_wall_time,
            (throughput(self.run_read_ops, self.run_read_time) as u64).separate_with_underscores(),
            r_p50,
            r_p95,
            r_p99,
            r_p999
        )?;

        writeln!(f, "=== RUN WRITE ===")?;
        write!(
            f,
            "ops: {} | time: {:.1?} | throughput: {} ops/s | p50: {} µs | p95: {} µs | p99: {} µs | p99.9: {} µs",
            self.run_write_ops.separate_with_underscores(),
            self.run_wall_time,
            (throughput(self.run_write_ops, self.run_write_time) as u64)
                .separate_with_underscores(),
            w_p50,
            w_p95,
            w_p99,
            w_p999
        )
    }
}

trait WorkloadConfig: Sync {
    fn get_name(&self) -> String;
    /// How many records to insert during load phase
    fn get_load_phase_insert_count(&self) -> u64;
    /// How many operations to execute in run phase
    fn get_operation_count(&self) -> u64;
    /// Of all the operations executed in run phase, what percentage are reads
    fn get_read_percent(&self) -> f64;
    fn get_write_percent(&self) -> f64;

    /// key size
    fn get_key_size(&self) -> u64;
    /// Range of value sizes
    fn get_value_size_range(&self) -> std::ops::Range<u64>;

    // add read mod write, -> tx
    // add scancount?
    /// How many threads to execute this workload on (total ops = thread_count*get_operation_count())
    fn get_thread_count(&self) -> u32;
}

pub trait Workload {
    fn init_stats(&self) -> Result<WorkloadStats> {
        WorkloadStats::new()
    }
    fn exec_load(&self, db: Arc<dyn Database>, stats: &mut WorkloadStats) -> Result<()>;
    fn exec_run(&self, db: Arc<dyn Database>, stats: &mut WorkloadStats) -> Result<()>;

    fn get_name(&self) -> String;
}

impl<T: WorkloadConfig + Sync> Workload for T {
    fn exec_load(&self, db: Arc<dyn Database>, stats: &mut WorkloadStats) -> Result<()> {
        validate_config(self);
        db.init()?;
        let time = load(&db, self)?;
        stats.load_time = time;
        stats.load_ops = self.get_load_phase_insert_count();
        Ok(())
    }

    fn exec_run(&self, db: Arc<dyn Database>, stats: &mut WorkloadStats) -> Result<()> {
        let mut read_duration = Duration::ZERO;
        let mut read_ops = 0;
        let mut read_hist = Histogram::<u64>::new_with_bounds(1, 10_000_000, 3)?;
        let mut write_duration = Duration::ZERO;
        let mut write_ops = 0;
        let mut write_hist = Histogram::<u64>::new_with_bounds(1, 10_000_000, 3)?;
        std::thread::scope(|s| {
            let mut handles = vec![];
            let start_time = Instant::now();
            for _ in 0..self.get_thread_count() {
                let h = s.spawn(|| run(&db, self));
                handles.push(h);
            }

            handles.into_iter().for_each(|h| {
                let d = h.join().unwrap().unwrap();
                read_duration += d.read_duration;
                write_duration += d.write_duration;
                read_ops += d.read_ops;
                write_ops += d.write_ops;
                read_hist.add(d.read_hist).unwrap();
                write_hist.add(d.write_hist).unwrap();
            });
            stats.run_wall_time = start_time.elapsed();
        });

        stats.run_read_ops = read_ops;
        stats.run_write_ops = write_ops;
        stats.run_read_time = read_duration;
        stats.run_write_time = write_duration;
        stats.run_read_hist_micro_sec = read_hist;
        stats.run_write_hist_micro_sec = write_hist;
        Ok(())
    }

    fn get_name(&self) -> String {
        self.get_name()
    }
}

fn load(db: &Arc<dyn Database>, config: &impl WorkloadConfig) -> Result<Duration> {
    let mut time = Duration::ZERO;
    let v_r = config.get_value_size_range();
    let mut value_size_gen = KVSizeGen::new(v_r.end - v_r.start, random())?;

    let key_size = config.get_key_size();
    let mut key_bytes = vec![0u8; key_size as usize];

    for i in 0..config.get_load_phase_insert_count() {
        let value_size = value_size_gen.get_size() + v_r.start;
        let mut value_bytes = vec![0u8; value_size as usize];

        let mut rng = SmallRng::seed_from_u64(i);
        rng.fill_bytes(&mut key_bytes);
        rng.fill_bytes(&mut value_bytes);

        let s = Instant::now();
        db.set(key_bytes.as_slice(), value_bytes.as_slice())?;
        time += s.elapsed()
    }

    Ok(time)
}

struct RunDuration {
    read_duration: Duration,
    read_ops: u64,
    read_hist: Histogram<u64>,
    write_duration: Duration,
    write_ops: u64,
    write_hist: Histogram<u64>,
}

fn run(db: &Arc<dyn Database>, config: &impl WorkloadConfig) -> Result<RunDuration> {
    let mut read_duration = Duration::ZERO;
    let mut read_ops = 0;
    let mut read_hist = Histogram::<u64>::new_with_bounds(1, 10_000_000, 3)?;

    let mut write_duration = Duration::ZERO;
    let mut write_ops = 0;
    let mut write_hist = Histogram::<u64>::new_with_bounds(1, 10_000_000, 3)?;

    let v_r = config.get_value_size_range();
    let mut value_size_gen = KVSizeGen::new(v_r.end - v_r.start, random())?;
    let mut bytes_gen = ByteGen::new(config.get_load_phase_insert_count(), random())?;
    let mut rng = rand::rng();

    let key_size = config.get_key_size();

    for _ in 0..config.get_operation_count() {
        let x: f64 = rng.random();
        let key_bytes = bytes_gen.get_key_bytes(key_size);
        if x < config.get_read_percent() {
            let start = Instant::now();
            db.get(key_bytes.as_slice())?;
            let mirco_sec = start.elapsed();
            read_duration += start.elapsed();
            read_hist.record(mirco_sec.as_micros() as u64)?;
            read_ops += 1;
        } else if x < config.get_read_percent() + config.get_write_percent() {
            let value_size = value_size_gen.get_size();
            let value_bytes = bytes_gen.get_value_bytes(value_size);
            let start = Instant::now();
            db.set(key_bytes.as_slice(), value_bytes.as_slice())?;
            let mirco_sec = start.elapsed();
            write_duration += start.elapsed();
            write_hist.record(mirco_sec.as_micros() as u64)?;
            write_ops += 1;
        } else {
            unreachable!("Should not get here");
        };
    }

    Ok(RunDuration {
        read_duration,
        read_ops,
        read_hist,
        write_duration,
        write_ops,
        write_hist,
    })
}

fn validate_config(config: &impl WorkloadConfig) {
    assert!(
        config.get_read_percent() >= 0.0,
        "Read percent must be larger than or equal to 0"
    );
    assert!(
        config.get_read_percent() <= 1.0,
        "Read percent must be less than or equal to 1"
    );

    assert!(
        config.get_write_percent() >= 0.0,
        "Write percent must be larger than or equal to 0"
    );
    assert!(
        config.get_write_percent() <= 1.0,
        "Write percent must be less than or equal to 1"
    );

    assert!(
        config.get_read_percent() + config.get_write_percent() > 0.0,
        "Read and write both cannot be zero percent"
    );
    assert!(
        config.get_read_percent() + config.get_write_percent() <= 1.0,
        "Read and write cannot not combine to above 1"
    );
}
