#![allow(non_snake_case)]
use snafu::Snafu;
use snafu::Backtrace;
use snafu::IntoError;
use std::path::PathBuf;
pub use snafu::ResultExt as SnafuResultExt;

pub use super::SE;

pub trait Mark1 {}
impl Mark1 for regex::Error {}
impl Mark1 for std::num::ParseIntError {}
impl Mark1 for serde_json::Error {}
impl Mark1 for http::Error {}
impl Mark1 for hyper::Error {}
impl Mark1 for tera::Error {}

impl<T: Mark1 + core::fmt::Debug> From<T> for Error {
  fn from(k: T) -> Self {
    Bad { msg: format!("{:?}", k) }.into_error(snafu::NoneError)
  }
}

impl From<std::io::Error> for Error {
  fn from(e: std::io::Error) -> Self {
    Bad { msg: format!("std::io::Error {}", e) }.into_error(snafu::NoneError)
  }
}

impl From<std::string::FromUtf8Error> for Error {
  fn from(e: std::string::FromUtf8Error) -> Self {
    Bad { msg: format!("FromUtf8Error {}", e) }.into_error(snafu::NoneError)
  }
}

impl From<tokio_postgres::Error> for Error {
  fn from(e: tokio_postgres::Error) -> Self {
    Bad { msg: format!("{}", e) }.into_error(snafu::NoneError)
  }
}

impl From<parsersdaq::Error> for Error {
  fn from(e: parsersdaq::Error) -> Self {
    Bad { msg: format!("{}", e) }.into_error(snafu::NoneError)
  }
}

impl From<tokio::task::JoinError> for Error {
  fn from(e: tokio::task::JoinError) -> Self {
    Bad { msg: format!("{}", e) }.into_error(snafu::NoneError)
  }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
  #[snafu(display("Could not open path {}: {}", path.display(), source))]
  OpenPath {
    source: std::io::Error,
    backtrace: Backtrace,
    path: PathBuf,
  },
  DbConnect {
    source: tokio_postgres::Error,
    backtrace: Backtrace,
  },
  DbError {
    source: tokio_postgres::Error,
    backtrace: Backtrace,
    file: String,
    line: u32,
  },
  StringConversion {
    backtrace: Backtrace,
    source: std::string::FromUtf8Error,
  },
  Bad {
    backtrace: Backtrace,
    msg: String,
  },
  #[snafu(display("ConfigFileTooLarge at {:?}", path))]
  ConfigFileTooLarge {
    backtrace: Backtrace,
    path: PathBuf,
  },
  ChannelDatadirNotFound {
    source: std::io::Error,
    backtrace: Backtrace,
    path: PathBuf,
  },
  ChannelConfigdirNotFound {
    source: std::io::Error,
    backtrace: Backtrace,
    path: PathBuf,
  },
  ExpectSingleRow {
    backtrace: Backtrace,
  },
  ExpectMaxOneRow {
    backtrace: Backtrace,
  },
  ConfigFileOnDiskShrunk {
    backtrace: Backtrace,
    path: PathBuf,
  },
  WithPos {
    source: tokio_postgres::Error,
    backtrace: Backtrace,
    file: String,
    line: u32,
  },
  AddPos {
    source: Box<Error>,
    backtrace: Backtrace,
    file: String,
    line: u32,
  },
  Timeout {
    backtrace: Backtrace,
    file: String,
    line: u32,
  },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error2 {
}

#[macro_export]
macro_rules! SE {
  ($x:ident) => (
    $x {
      file: file!(),
      line: line!(),
    }
  )
}

pub trait ResultExt<T, E> {
  fn ctx<C, W>(self, ctx: C) -> Result<T, W>
  where C: snafu::IntoError<W, Source=E>, W: std::error::Error + snafu::ErrorCompat;
  fn ctxb<C, W>(self, ctx: C) -> Result<T, W>
  where C: snafu::IntoError<W, Source=Box<E>>, W: std::error::Error + snafu::ErrorCompat;
}

impl<T, E> ResultExt<T, E> for Result<T, E> {
  fn ctx<C, W>(self, ctx: C) -> Result<T, W>
  where C: snafu::IntoError<W, Source=E>, W: std::error::Error + snafu::ErrorCompat
  {
    use snafu::ResultExt;
    self.context(ctx)
  }
  fn ctxb<C, W>(self, ctx: C) -> Result<T, W>
  where C: snafu::IntoError<W, Source=Box<E>>, W: std::error::Error + snafu::ErrorCompat
  {
    use snafu::ResultExt;
    self.map_err(|x|Box::new(x)).context(ctx)
  }
}
