use na::coordinates::{IJKW, XYZ};
use nalgebra as na;
use r2r::geometry_msgs::msg::{Quaternion, Transform, TransformStamped, Vector3};

pub trait TransformNalgebraExt {
    fn from_na_isometry3(transform: &na::Isometry3<f64>) -> Self;
    fn to_na_isometry3(&self) -> na::Isometry3<f64>;
}

impl TransformNalgebraExt for Transform {
    fn from_na_isometry3(isometry: &na::Isometry3<f64>) -> Self {
        let na::Isometry3 {
            rotation,
            translation,
        } = isometry;
        let IJKW { i, j, k, w } = ***rotation;
        let XYZ { x, y, z } = **translation;

        Self {
            translation: Vector3 { x, y, z },
            rotation: Quaternion {
                x: i,
                y: j,
                z: k,
                w,
            },
        }
    }

    fn to_na_isometry3(&self) -> na::Isometry3<f64> {
        let Self {
            translation: Vector3 { x, y, z },
            rotation:
                Quaternion {
                    x: i,
                    y: j,
                    z: k,
                    w,
                },
        } = *self;

        let t = na::Translation3::new(x, y, z);
        let rot: na::UnitQuaternion<_> = na::Unit::new_normalize(na::Quaternion::new(w, i, j, k));
        na::Isometry3::from_parts(t, rot)
    }
}

pub trait TransformStampedNalgebraExt {
    fn to_na_isometry3(&self) -> na::Isometry3<f64>;
}

impl TransformStampedNalgebraExt for TransformStamped {
    fn to_na_isometry3(&self) -> na::Isometry3<f64> {
        self.transform.to_na_isometry3()
    }
}
