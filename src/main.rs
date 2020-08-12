use serde_json::Value;
use std::convert::TryInto;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Opt {
    input: PathBuf,

    /// Sets compression level (1-21), defaults to zstd's default (currently 3)
    #[structopt(short, long)]
    level: Option<i32>,

    /// Disables compression
    #[structopt(short, long)]
    no_compression: bool,

    /// Enable multithread and set number of workers (0 defaults to number of cores)
    #[structopt(short, long)]
    multithread: Option<u32>,
}

fn main() -> std::io::Result<()> {
    let opt = Opt::from_args();
    let mut output = opt.input.clone();
    let extension = if opt.no_compression {
        "json.bin"
    } else {
        "json.bin.zstd"
    };
    output.set_extension(extension);

    let f = File::open(opt.input)?;
    let reader = BufReader::new(f);

    let v: Value = serde_json::from_reader(reader)?;

    let of = File::create(output)?;
    let writer = BufWriter::new(of);

    if opt.no_compression {
        bincode::serialize_into(writer, &v).expect("bincode error");
    } else {
        let mut zstd_encoder = zstd::stream::write::Encoder::new(writer, opt.level.unwrap_or(0))?;

        if let Some(n_workers) = opt.multithread {
            let n_workers: u32 = if n_workers == 0 {
                // more than u32::MAX cpus? unlikely
                num_cpus::get().try_into().unwrap()
            } else {
                n_workers
            };
            eprintln!("using {} workers...", n_workers);
            zstd_encoder.multithread(n_workers)?;
        }

        let zstd_encoder = zstd_encoder.auto_finish();

        bincode::serialize_into(zstd_encoder, &v).expect("bincode error");
    }

    Ok(())
}
