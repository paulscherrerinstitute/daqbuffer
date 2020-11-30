use snafu::Snafu;
use nom::error::ErrorKind;

#[derive(Debug)]
pub struct E2 {
  inplen: usize,
  msg: String,
}

impl nom::error::ParseError<&[u8]> for E2 {
  fn from_error_kind(input: &[u8], kind: ErrorKind) -> Self {
    E2 {
      inplen: input.len(),
      msg: format!("kind {:?}", kind),
    }
  }
  fn append(input: &[u8], kind: ErrorKind, _this: Self) -> Self {
    E2 {
      inplen: input.len(),
      msg: format!("kind {:?}", kind),
    }
  }
}

pub fn BadError<O, T: Into<String>>(msg: T) -> Result<O, Error> {
  Err(Error::Bad {
    msg: msg.into(),
  })
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
  #[snafu(display("nom error: {:?}", source))]
  NomError {
    source: nom::Err<E2>,
  },
  Bad {
    msg: String,
  },
}

impl From<nom::Err<(&[u8], ErrorKind)>> for Error {
  fn from(k: nom::Err<(&[u8], ErrorKind)>) -> Error {
    use snafu::IntoError;
    match k {
      nom::Err::Error(e) => NomError{}.into_error(nom::Err::Error(E2 {
        inplen: 0,
        msg: format!("Error: {:?}", e),
      })),
      nom::Err::Failure(e) => NomError{}.into_error(nom::Err::Failure(E2 {
        inplen: 0,
        msg: format!("Failure: {:?}", e),
      })),
      nom::Err::Incomplete(e) => NomError{}.into_error(nom::Err::Incomplete(e)),
    }
  }
}

impl From<std::string::FromUtf8Error> for Error {
  fn from(e: std::string::FromUtf8Error) -> Self {
    Error::Bad { msg: format!("FromUtf8Error {}", e) }
  }
}
