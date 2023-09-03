//! Extensions for r2r Message Types.
//!
//! This crate extends common ROS message types, provided by [r2r],
//! with type conversion to data types from third-party crates. The
//! following crates are supported.
//!
//! - [nalgebra](https://docs.rs/nalgebra/)
//! - [opencv](https://docs.rs/opencv/)
//! - [arrow](https://docs.rs/arrow/)

pub mod geometry_msgs;
pub mod sensor_msgs;

pub mod prelude {
    pub use crate::geometry_msgs::msg::*;
    pub use crate::sensor_msgs::msg::*;
}
