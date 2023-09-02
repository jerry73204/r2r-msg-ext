use r2r::sensor_msgs::msg::PointCloud2;
use std::slice::Chunks;

pub type PointBytesIter<'a> = Box<dyn Iterator<Item = Chunks<'a, u8>> + Sync + Send + 'a>;

pub trait PointCloud2Ext {
    fn row_bytes_iter(&self) -> Chunks<'_, u8>;
    fn point_bytes_iter(&self) -> PointBytesIter<'_>;
}

impl PointCloud2Ext for PointCloud2 {
    fn row_bytes_iter(&self) -> Chunks<'_, u8> {
        let Self {
            row_step, ref data, ..
        } = *self;

        data.chunks(row_step as usize)
    }

    fn point_bytes_iter(&self) -> PointBytesIter<'_> {
        let iter = self
            .row_bytes_iter()
            .map(|row| row.chunks(self.point_step as usize));
        Box::new(iter)
    }
}
