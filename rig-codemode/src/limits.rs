//! Host-configured resource ceilings and per-request restrictions.
//!
//! Pass [`Limits`] to [`crate::CodeModeBuilder::limits`]. Each request can
//! lower these ceilings with [`crate::ExecutionRequest::with_limits`]. A
//! request cannot raise them. Lowering `max_calls` also lowers concurrency
//! when needed; it never increases the host's `max_in_flight`.
//!
//! ```
//! use std::time::Duration;
//! use rig_codemode::{ExecutionRequest, LimitOverrides, Limits, LimitsError};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let host_limits = Limits {
//!     wall_time: Duration::from_secs(5),
//!     max_calls: 16,
//!     max_in_flight: 4,
//!     ..Limits::default()
//! };
//! let request = ExecutionRequest::new("text('ready');")
//!     .with_parent_call_id("request-17")
//!     .with_limits(LimitOverrides {
//!         wall_time: Some(Duration::from_secs(1)),
//!         max_calls: Some(2),
//!         ..LimitOverrides::default()
//!     });
//! let effective = host_limits.restrict(&request.limits)?;
//! assert_eq!(effective.max_calls, 2);
//! assert_eq!(effective.max_in_flight, 2);
//! assert!(matches!(
//!     host_limits.restrict(&LimitOverrides {
//!         max_calls: Some(17),
//!         ..LimitOverrides::default()
//!     }),
//!     Err(LimitsError::AboveCeiling { .. })
//! ));
//! # Ok(())
//! # }
//! ```
//!
//! Wall time includes queued and in-flight tool calls, but excludes
//! [`crate::ScriptPolicy`] review. Guest heap limits do not bound memory used
//! inside host tools. Host tools must also bound their own I/O and allocation.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Resource ceilings for one script execution. The host sets these; a request
/// may lower them with [`LimitOverrides`] but never raise them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    /// Maximum script source size in UTF-8 bytes.
    pub source_bytes: usize,
    /// Guest heap limit in bytes.
    pub memory_bytes: usize,
    /// Whole-script wall time, including queued and in-flight tool calls.
    pub wall_time: Duration,
    /// Total tool calls one script may start.
    pub max_calls: u32,
    /// Tool calls dispatched concurrently; further calls wait in order.
    pub max_in_flight: u32,
    /// Maximum size of one serialized argument or result message in bytes.
    pub message_bytes: usize,
    /// Maximum total bytes emitted through `text()`.
    pub output_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            source_bytes: 64 * 1024,
            memory_bytes: 64 * 1024 * 1024,
            wall_time: Duration::from_secs(30),
            max_calls: 64,
            max_in_flight: 8,
            message_bytes: 1024 * 1024,
            output_bytes: 64 * 1024,
        }
    }
}

/// A limit was zero, inconsistent, or a request asked for more than the host
/// ceiling.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LimitsError {
    /// Every limit must be greater than zero.
    #[error("limit `{0}` must be greater than zero")]
    Zero(&'static str),
    /// `max_in_flight` cannot exceed `max_calls`.
    #[error("max_in_flight ({in_flight}) exceeds max_calls ({calls})")]
    InFlightExceedsCalls {
        /// Configured concurrency.
        in_flight: u32,
        /// Configured call budget.
        calls: u32,
    },
    /// A request asked to raise a limit above the host ceiling.
    #[error("requested {name} ({requested}) exceeds the host ceiling ({ceiling})")]
    AboveCeiling {
        /// Which limit.
        name: &'static str,
        /// The requested value, rendered.
        requested: String,
        /// The ceiling, rendered.
        ceiling: String,
    },
}

impl Limits {
    /// Reject zero or inconsistent limits.
    pub fn validate(&self) -> Result<(), LimitsError> {
        if self.source_bytes == 0 {
            return Err(LimitsError::Zero("source_bytes"));
        }
        if self.memory_bytes == 0 {
            return Err(LimitsError::Zero("memory_bytes"));
        }
        if self.wall_time.is_zero() {
            return Err(LimitsError::Zero("wall_time"));
        }
        if self.max_calls == 0 {
            return Err(LimitsError::Zero("max_calls"));
        }
        if self.max_in_flight == 0 {
            return Err(LimitsError::Zero("max_in_flight"));
        }
        if self.message_bytes == 0 {
            return Err(LimitsError::Zero("message_bytes"));
        }
        if self.output_bytes == 0 {
            return Err(LimitsError::Zero("output_bytes"));
        }
        if self.max_in_flight > self.max_calls {
            return Err(LimitsError::InFlightExceedsCalls {
                in_flight: self.max_in_flight,
                calls: self.max_calls,
            });
        }
        Ok(())
    }

    /// Apply per-request restrictions. Each requested value must be at or
    /// below this ceiling; the result is validated.
    pub fn restrict(&self, overrides: &LimitOverrides) -> Result<Self, LimitsError> {
        let mut limits = *self;
        if let Some(wall_time) = overrides.wall_time {
            if wall_time > self.wall_time {
                return Err(above("wall_time", wall_time, self.wall_time));
            }
            limits.wall_time = wall_time;
        }
        if let Some(max_calls) = overrides.max_calls {
            if max_calls > self.max_calls {
                return Err(above("max_calls", max_calls, self.max_calls));
            }
            limits.max_calls = max_calls;
            limits.max_in_flight = limits.max_in_flight.min(max_calls);
        }
        if let Some(memory_bytes) = overrides.memory_bytes {
            if memory_bytes > self.memory_bytes {
                return Err(above("memory_bytes", memory_bytes, self.memory_bytes));
            }
            limits.memory_bytes = memory_bytes;
        }
        if let Some(output_bytes) = overrides.output_bytes {
            if output_bytes > self.output_bytes {
                return Err(above("output_bytes", output_bytes, self.output_bytes));
            }
            limits.output_bytes = output_bytes;
        }
        limits.validate()?;
        Ok(limits)
    }
}

fn above<T: std::fmt::Debug>(name: &'static str, requested: T, ceiling: T) -> LimitsError {
    LimitsError::AboveCeiling {
        name,
        requested: format!("{requested:?}"),
        ceiling: format!("{ceiling:?}"),
    }
}

/// Lower limits requested for one execution. Absent fields keep the host
/// ceiling.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LimitOverrides {
    /// Shorter wall time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_time: Option<Duration>,
    /// Smaller call budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_calls: Option<u32>,
    /// Smaller guest heap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<usize>,
    /// Smaller output budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_bytes: Option<usize>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn defaults_validate() {
        Limits::default().validate().unwrap();
    }

    #[test]
    fn overrides_only_lower() {
        let ceiling = Limits::default();
        let lowered = ceiling
            .restrict(&LimitOverrides {
                wall_time: Some(Duration::from_secs(1)),
                max_calls: Some(4),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(lowered.wall_time, Duration::from_secs(1));
        assert_eq!(lowered.max_calls, 4);
        assert_eq!(
            lowered.max_in_flight, 4,
            "in-flight follows a smaller call budget"
        );

        let raised = ceiling.restrict(&LimitOverrides {
            wall_time: Some(Duration::from_secs(31)),
            ..Default::default()
        });
        assert!(matches!(
            raised,
            Err(LimitsError::AboveCeiling {
                name: "wall_time",
                ..
            })
        ));
        let zero = ceiling.restrict(&LimitOverrides {
            max_calls: Some(0),
            ..Default::default()
        });
        assert_eq!(zero, Err(LimitsError::Zero("max_calls")));
    }
}
