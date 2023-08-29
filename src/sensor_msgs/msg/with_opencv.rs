use anyhow::ensure;
use anyhow::{bail, Result};
use opencv::core::Scalar;
use opencv::core::Vec3b;
use opencv::core::VecN;
use opencv::core::CV_8UC3;
use opencv::prelude::*;
use r2r::sensor_msgs::msg::Image;

pub trait ImageOpenCvExt {
    fn to_mat(&self) -> Result<Mat>;
}

impl ImageOpenCvExt for Image {
    fn to_mat(&self) -> Result<Mat> {
        image_to_mat(self)
    }
}

/// Converts a ROS image to an OpenCV Mat.
fn image_to_mat(image: &Image) -> Result<Mat> {
    let mat = match image.encoding.as_str() {
        "BGR8" => bgr8_to_mat(image)?,
        "RGB8" => rgb8_to_mat(image)?,
        "UYVY" => uyvy_to_mat(image)?,
        _ => bail!("unsupported image format '{}'", image.encoding),
    };

    Ok(mat)
}

fn bgr8_to_mat(image: &Image) -> Result<Mat> {
    let Image {
        height,
        width,
        step: row_step,
        ref data,
        ..
    } = *image;

    let is_bigendian = image.is_bigendian != 0;
    ensure!(!is_bigendian);
    let pixel_step = 3;
    ensure!(row_step == width * pixel_step);
    ensure!(data.len() == (row_step * height) as usize);

    let mut mat =
        Mat::new_rows_cols_with_default(height as i32, width as i32, CV_8UC3, Scalar::all(0.0))?;

    data.chunks_exact(3).enumerate().for_each(|(pidx, bytes)| {
        let col = pidx % width as usize;
        let row = pidx / width as usize;
        let pixel: &mut Vec3b = mat.at_2d_mut(row as i32, col as i32).unwrap();
        let bytes: [u8; 3] = bytes.try_into().unwrap();
        *pixel = VecN(bytes);
    });

    Ok(mat)
}

fn rgb8_to_mat(image: &Image) -> Result<Mat> {
    let Image {
        height,
        width,
        step: row_step,
        ref data,
        ..
    } = *image;

    let is_bigendian = image.is_bigendian != 0;
    ensure!(!is_bigendian);
    let pixel_step = 3;
    ensure!(row_step == width * pixel_step);
    ensure!(data.len() == (row_step * height) as usize);

    let mut mat =
        Mat::new_rows_cols_with_default(height as i32, width as i32, CV_8UC3, Scalar::all(0.0))?;

    data.chunks_exact(3).enumerate().for_each(|(pidx, bytes)| {
        let col = pidx % width as usize;
        let row = pidx / width as usize;
        let pixel: &mut Vec3b = mat.at_2d_mut(row as i32, col as i32).unwrap();
        let [r, g, b]: [u8; 3] = bytes.try_into().unwrap();
        *pixel = VecN([b, g, r]);
    });

    Ok(mat)
}

#[cfg(feature = "nightly")]
fn uyvy_to_mat(image: &Image) -> Result<Mat> {
    unsafe {
        let Image {
            height,
            width,
            step: row_step,
            ref data,
            ..
        } = *image;

        let is_bigendian = image.is_bigendian != 0;
        ensure!(!is_bigendian);
        let pixel_step = 2;
        ensure!(row_step == width * pixel_step);
        ensure!(data.len() == (row_step * height) as usize);

        let mut mat = Mat::new_rows_cols(height as i32, width as i32, CV_8UC3)?;
        let uyvy422_buf = data;
        let bgr24_buf =
            std::slice::from_raw_parts_mut(mat.data_mut(), (height * width * 3) as usize);
        fast_yuv442_to_rgb24::uvy422_to_bgr24::uyvy422_to_bgr24_chunk16_many(
            uyvy422_buf,
            bgr24_buf,
        );
        Ok(mat)
    }
}

#[cfg(not(feature = "nightly"))]
fn uyvy_to_mat(_image: &Image) -> Result<Mat> {
    bail!("UYVY image to OpenCV Mat is not implemented for stable version");
}
