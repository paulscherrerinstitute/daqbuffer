use serde_derive::{Serialize, Deserialize};

pub trait BackendAware {
  fn backend(&self) -> &str;
}

pub trait FromErrorCode {
  fn from_error_code(backend: &str, code: ErrorCode) -> Self;
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ErrorCode {
  Error,
  Timeout,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ErrorDescription {
  code: ErrorCode,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Ordering {
  #[serde(rename = "none")]
  NONE,
  #[serde(rename = "asc")]
  ASC,
  #[serde(rename = "desc")]
  DESC,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelSearchQuery {
  #[serde(skip_serializing_if = "Option::is_none")]
  pub regex: Option<String>,
  #[serde(rename = "sourceRegex", skip_serializing_if = "Option::is_none")]
  pub source_regex: Option<String>,
  #[serde(rename = "descriptionRegex", skip_serializing_if = "Option::is_none")]
  pub description_regex: Option<String>,
  #[serde(skip_serializing_if = "Vec::is_empty")]
  pub backends: Vec<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub ordering: Option<Ordering>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelSearchResultItem {
  pub backend: String,
  pub channels: Vec<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub error: Option<ErrorDescription>,
}

impl BackendAware for ChannelSearchResultItem {
  fn backend(&self) -> &str {
    &self.backend
  }
}

impl FromErrorCode for ChannelSearchResultItem {
  fn from_error_code(backend: &str, code: ErrorCode) -> Self {
    Self {
      backend: backend.into(),
      channels: vec![],
      error: Some(ErrorDescription{code}),
    }
  }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelSearchResult(pub Vec<ChannelSearchResultItem>);



#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelConfigsQuery {
  #[serde(skip_serializing_if = "Option::is_none")]
  pub regex: Option<String>,
  #[serde(rename = "sourceRegex")]
  pub source_regex: Option<String>,
  #[serde(rename = "descriptionRegex")]
  pub description_regex: Option<String>,
  #[serde(skip_serializing_if = "Vec::is_empty", default)]
  pub backends: Vec<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub ordering: Option<Ordering>,
}


#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelConfigsResponse(pub Vec<ChannelBackendConfigs>);

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelBackendConfigs {
  pub backend: String,
  pub channels: Vec<ChannelConfig>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub error: Option<ErrorDescription>,
}

impl BackendAware for ChannelBackendConfigs {
  fn backend(&self) -> &str {
    &self.backend
  }
}

impl FromErrorCode for ChannelBackendConfigs {
  fn from_error_code(backend: &str, code: ErrorCode) -> Self {
    Self {
      backend: backend.into(),
      channels: vec![],
      error: Some(ErrorDescription{code}),
    }
  }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelConfig {
  pub backend: String,
  pub name: String,
  pub source: String,
  #[serde(rename = "type")]
  pub ty: String,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub shape: Option<Vec<u32>>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub unit: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub description: Option<String>,
}
