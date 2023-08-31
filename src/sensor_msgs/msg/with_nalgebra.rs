use anyhow::{bail, Result};
use nalgebra as na;
use r2r::sensor_msgs::msg::{PointCloud2, PointField};

pub trait PointCloud2NalgebraExt {
    fn na_point_iter(&self)
        -> Result<Box<dyn Iterator<Item = na::Point3<f32>> + Sync + Send + '_>>;

    fn to_na_point_vec(&self) -> Result<Vec<na::Point3<f32>>> {
        Ok(self.na_point_iter()?.collect())
    }
}

impl PointCloud2NalgebraExt for PointCloud2 {
    fn na_point_iter(
        &self,
    ) -> Result<Box<dyn Iterator<Item = na::Point3<f32>> + Sync + Send + '_>> {
        let iter = pointcloud2_to_na_point_iter(self)?;
        Ok(Box::new(iter))
    }
}

/// Converts a ROS point cloud to an iterator of nalgebra points.
pub fn pointcloud2_to_na_point_iter(
    pcd: &PointCloud2,
) -> Result<impl Iterator<Item = na::Point3<f32>> + Sync + Send + '_> {
    let is_big_endian = pcd.is_bigendian;

    // Assert the point cloud has at least 4 fields. Otherwise return error.
    let [fx, fy, fz] = match pcd.fields.get(0..3) {
        Some([fx, fy, fz]) => [fx, fy, fz],
        Some(_) => unreachable!(),
        None => {
            bail!("Ignore a point cloud message with less then 3 fields");
        }
    };

    // Assert the fields are named x, y, z and intensity. Otherwise return error.
    match (fx.name.as_str(), fy.name.as_str(), fz.name.as_str()) {
        ("x", "y", "z") => {}
        _ => {
            bail!("Ignore a point cloud message with incorrect field name");
        }
    }

    // Assert each field has f32 type and contains a single
    // value. Otherwise it returns an error.
    let check_field = |field: &PointField| {
        match field {
            PointField {
                datatype: 7,
                count: 1,
                ..
            } => {}
            _ => {
                bail!("Ignore a point cloud message with non-f64 or non-single-value values");
            }
        }

        anyhow::Ok(())
    };

    check_field(fx)?;
    check_field(fy)?;
    check_field(fz)?;

    // Assert a point is 12 bytes (3 * f32 values). Otherwise, return error.
    if pcd.point_step < 12 {
        bail!("Ignore a point cloud message with incorrect point_step (expect 16)");
    }

    let parse_f32 = move |slice: &[u8]| {
        let array: [u8; 4] = slice.try_into().unwrap();

        if is_big_endian {
            f32::from_be_bytes(array)
        } else {
            f32::from_le_bytes(array)
        }
    };

    // Transform the data byte to a vec of points.
    let iter = pcd
        .data
        .chunks(pcd.point_step as usize)
        .map(move |point_bytes| {
            let xbytes = &point_bytes[0..4];
            let ybytes = &point_bytes[4..8];
            let zbytes = &point_bytes[8..12];
            let x = parse_f32(xbytes);
            let y = parse_f32(ybytes);
            let z = parse_f32(zbytes);
            let position = na::Point3::new(x, y, z);
            position
        });

    Ok(iter)
}
