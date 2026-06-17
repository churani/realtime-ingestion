use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::{Datelike, Local, Timelike};

struct Inner {
    file: File,
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
}

impl Inner {
    fn open(base: &Path, year: i32, month: u32, day: u32, hour: u32) -> io::Result<Self> {
        let dir = base
            .join(year.to_string())
            .join(month.to_string())
            .join(day.to_string());
        fs::create_dir_all(&dir)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(format!("{hour}.log")))?;
        Ok(Self { file, year, month, day, hour })
    }
}

pub struct HourlyWriter {
    base: PathBuf,
    inner: Arc<Mutex<Inner>>,
}

impl HourlyWriter {
    pub fn new(base: impl Into<PathBuf>) -> io::Result<Self> {
        let base = base.into();
        let now = Local::now();
        let inner = Inner::open(&base, now.year(), now.month(), now.day(), now.hour())?;
        Ok(Self {
            base,
            inner: Arc::new(Mutex::new(inner)),
        })
    }
}

impl Write for HourlyWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let now = Local::now();
        let (year, month, day, hour) = (now.year(), now.month(), now.day(), now.hour());
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.year != year || inner.month != month || inner.day != day || inner.hour != hour {
            if let Ok(new_inner) = Inner::open(&self.base, year, month, day, hour) {
                *inner = new_inner;
            }
        }
        inner.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).file.flush()
    }
}
