use std::ops::Range;
use crate::workload::{WorkloadConfig};

#[derive(Default)]
pub struct ReadWrite;

impl WorkloadConfig for ReadWrite {
    fn get_name(&self) -> String {
        "ReadWrite".to_owned()
    }

    fn get_load_phase_insert_count(&self) -> u64 {
        10_000
    }

    fn get_operation_count(&self) -> u64 {
        8_000
    }

    fn get_read_percent(&self) -> f64 {
        0.5
    }

    fn get_write_percent(&self) -> f64 {
         1.0 - self.get_read_percent()
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
