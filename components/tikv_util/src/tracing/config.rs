use crate::config::{check_addr, ReadableDuration};
use configuration::Configuration;
use serde::{Deserialize, Serialize};
use std::error::Error;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub jaeger_thrift_compact_agent: String,
    pub num_report_threads: usize,
    pub duration_threshold: ReadableDuration,
    pub spans_max_length: usize,
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if !self.jaeger_thrift_compact_agent.is_empty() {
            check_addr(&self.jaeger_thrift_compact_agent)?;
            if self.num_report_threads == 0 {
                return Err("tracing.num_threads cannot be 0".into());
            }
            if self.spans_max_length == 0 {
                return Err("tracing.spans_max_length cannot be 0".into());
            }
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            jaeger_thrift_compact_agent: "".to_owned(),
            num_report_threads: 1,
            duration_threshold: ReadableDuration::millis(100),
            spans_max_length: 1000,
        }
    }
}
