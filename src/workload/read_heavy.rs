use crate::workload::WorkloadConfig;
use std::ops::Range;

#[derive(Default)]
pub struct ReadHeavy;

impl WorkloadConfig for ReadHeavy {
    fn get_name(&self) -> String {
        "ReadHeavy".to_string()
    }

    fn get_load_phase_insert_count(&self) -> u64 {
        10_000
    }

    fn get_operation_count(&self) -> u64 {
        8_000
    }

    fn get_read_percent(&self) -> f64 {
        0.95
    }

    fn get_write_percent(&self) -> f64 {
        0.05
    }

    fn get_key_size(&self) -> u64 {
        128
    }

    fn get_value_size_range(&self) -> Range<u64> {
        512..1024
    }

    fn get_thread_count(&self) -> u32 {
        16
    }
}
