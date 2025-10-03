use anyhow::Result;
use rand::distr::Distribution;
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use rand_distr::Zipf;

pub struct KVSizeGen {
    zipf: Zipf<f64>,
    rng: SmallRng,
}

pub struct ByteGen {
    zipf: Zipf<f64>,
    rng: SmallRng,
}

impl KVSizeGen {
    pub fn new(range: u64, seed: u64) -> Result<Self> {
        let g = Zipf::new(range as f64, 1.0)?;
        Ok(KVSizeGen { zipf: g, rng: SmallRng::seed_from_u64(seed) })
    }

    pub fn get_size(&mut self) -> u64 {
        self.zipf.sample(&mut self.rng) as u64
    }
}

impl ByteGen {
    pub fn new(range: u64, seed: u64) -> Result<Self> {
        let g = Zipf::new(range as f64, 1.0)?;
        Ok(ByteGen { zipf: g, rng: SmallRng::seed_from_u64(seed) })
    }

    pub fn get_key_bytes(&mut self, size: u64) -> Vec<u8> {
        let idx = self.zipf.sample(&mut self.rng) as u64;
        let mut bytes = vec![0u8; size as usize];

        SmallRng::seed_from_u64(idx).fill_bytes(&mut bytes[..]);
        bytes
    }

    pub fn get_value_bytes(&mut self, size: u64) -> Vec<u8> {
        let mut bytes = vec![0u8; size as usize];
        self.rng.fill_bytes(&mut bytes[..]);
        bytes
    }
}
