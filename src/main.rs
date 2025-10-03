mod database;
mod workload;
mod generator;

use crate::database::get_db;
use crate::workload::Workload;
use crate::WorkloadType::ReadWrite;
use anyhow::Result;
use clap::{Parser, ValueEnum};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(value_enum)]
    workload: WorkloadType,

    #[arg(value_enum)]
    database: DatabaseType,

    #[arg(short = 't', default_value = "1")]
    threads: u64,

    /// Optional properties
    #[arg(short = 'p')]
    properties: Option<String>,
}

#[derive(Copy, Clone, ValueEnum)]
enum WorkloadType {
    ReadWrite,
    ReadHeavy,
    ReadOnly,
    RangeScan,
}

#[derive(Copy, Clone, ValueEnum)]
// Update database::get_db when adding new variation
enum DatabaseType {
    MemBtree,
    Redb,
}

fn main() -> Result<()> {
    //let cli = Cli::parse();
    let workload = ReadWrite;
    let database = get_db(DatabaseType::Redb)?;

    let wl = get_wl(workload);
    let mut stats = wl.init_stats()?;
    wl.exec_load(database.clone(), &mut stats)?;
    wl.exec_run(database, &mut stats)?;
    println!("{}", stats);
    Ok(())
}

fn get_wl(wl: WorkloadType) -> Box<dyn Workload> {
    match wl {
        ReadWrite => Box::new(workload::read_write::ReadWrite),
        WorkloadType::ReadHeavy => todo!(),
        WorkloadType::ReadOnly => todo!(),
        WorkloadType::RangeScan => todo!(),
    }
}
