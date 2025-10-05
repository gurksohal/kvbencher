mod database;
mod generator;
mod workload;

use crate::WorkloadType::ReadWrite;
use crate::database::get_db;
use crate::workload::Workload;
use anyhow::Result;
use clap::{Parser, ValueEnum};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(value_enum)]
    workload: WorkloadType,

    #[arg(value_enum)]
    database: DatabaseType,

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
    Sled,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let workload = cli.workload;
    let database = get_db(cli.database)?;

    let wl = get_wl(workload);
    let mut stats = wl.init_stats()?;
    wl.exec_load(database.clone(), &mut stats)?;
    wl.exec_run(database, &mut stats)?;
    println!(
        "database: {}, workload: {}",
        get_db_name(cli.database),
        wl.get_name()
    );
    println!("==============================");
    println!("{}", stats);
    Ok(())
}

fn get_wl(wl: WorkloadType) -> Box<dyn Workload> {
    match wl {
        ReadWrite => Box::new(workload::read_write::ReadWrite),
        WorkloadType::ReadHeavy => Box::new(workload::read_heavy::ReadHeavy),
        WorkloadType::ReadOnly => Box::new(workload::read_only::ReadOnly),
        WorkloadType::RangeScan => todo!(),
    }
}

fn get_db_name(db: DatabaseType) -> String {
    match db {
        DatabaseType::MemBtree => "MemBtree".to_string(),
        DatabaseType::Redb => "Redb".to_string(),
        DatabaseType::Sled => "Sled".to_string(),
    }
}
