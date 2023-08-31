use anyhow::anyhow;
use anyhow::ensure;
use anyhow::Result;
use arrow::array::ArrayData;
use arrow::array::ArrayRef;
use arrow::array::FixedSizeListArray;
use arrow::array::Float32Array;
use arrow::array::Float64Array;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int8Array;
use arrow::array::StructArray;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt8Array;
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use itertools::Itertools;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use r2r::sensor_msgs::msg::PointCloud2;
use r2r::sensor_msgs::msg::PointField;
use std::sync::Arc;

macro_rules! make_array {
    ($iter:ident, $elem_ty:ty, $arr_ty:ty, $is_bigendian:expr) => {{
        let vec: Vec<_> = if $is_bigendian {
            $iter
                .map(|bytes| {
                    let array = bytes.try_into().unwrap();
                    let elem = <$elem_ty>::from_be_bytes(array);
                    Some(elem)
                })
                .collect()
        } else {
            $iter
                .map(|bytes| {
                    let array = bytes.try_into().unwrap();
                    let elem = <$elem_ty>::from_le_bytes(array);
                    Some(elem)
                })
                .collect()
        };

        Arc::new(<$arr_ty>::from(vec)) as ArrayRef
    }};
}

macro_rules! make_list_array {
    ($iter:ident, $name:expr, $count:expr, $elem_ty:ty, $data_type:path, $is_bigendian:expr) => {{
        let vec: Vec<_> = if $is_bigendian {
            $iter
                .map(|bytes| {
                    let array = bytes.try_into().unwrap();
                    let elem = <$elem_ty>::from_be_bytes(array);
                    elem
                })
                .collect()
        } else {
            $iter
                .map(|bytes| {
                    let array = bytes.try_into().unwrap();
                    let elem = <$elem_ty>::from_le_bytes(array);
                    elem
                })
                .collect()
        };
        let buf = Buffer::from_vec(vec);
        let value_data = ArrayData::builder($data_type).add_buffer(buf).build()?;

        let list_data_type = DataType::FixedSizeList(
            Arc::new(Field::new($name, $data_type, false)),
            $count as i32,
        );
        let list_data = ArrayData::builder(list_data_type)
            .add_child_data(value_data)
            .build()?;
        let list_array = FixedSizeListArray::from(list_data);

        Arc::new(list_array) as ArrayRef
    }};
}

pub trait PointCloud2ArrowExt {
    fn to_arrow_array(&self) -> Result<StructArray>;
}

impl PointCloud2ArrowExt for PointCloud2 {
    fn to_arrow_array(&self) -> Result<StructArray> {
        let PointCloud2 {
            ref fields,
            ref data,
            point_step,
            row_step,
            height,
            width,
            is_bigendian: is_be,
            ..
        } = *self;
        let width = width as usize;
        let height = height as usize;
        let point_step = point_step as usize;
        let row_step = row_step as usize;

        ensure!(
            width * point_step <= row_step,
            "Assertion width * point_step <= row_step failed"
        );
        ensure!(
            data.len() == row_step * height,
            "Invalid data size. Expect {} bytes, but get {} bytes.",
            row_step * height,
            data.len()
        );

        let fields: Vec<_> = fields
            .iter()
            .map(|field| -> Result<_> {
                let PointField {
                    ref name,
                    offset,
                    datatype,
                    count,
                } = *field;

                let datatype = PointCloud2DataType::from_u8(datatype)
                    .ok_or_else(|| anyhow!("Unsupported datatype {datatype}"))?;
                let arrow_datatype = datatype.to_arrow_datatype();
                let size = arrow_datatype.size();
                let field = Field::new(name, arrow_datatype, false);

                Ok(FieldDesc {
                    field,
                    datatype,
                    offset: offset as usize,
                    data_size: size,
                    count: count as usize,
                })
            })
            .try_collect()?;

        let point_bytes_iter = || {
            data.chunks(row_step)
                .flat_map(|row| row[0..(point_step * width)].chunks(point_step))
        };

        let columns: Vec<_> = fields
            .into_iter()
            .map(|field| -> Result<_> {
                let FieldDesc {
                    field,
                    datatype,
                    offset,
                    data_size,
                    count,
                } = field;
                let name = field.name();

                let elem_iter = point_bytes_iter().flat_map(|point_bytes| {
                    let start = offset;
                    let end = start + data_size * count;
                    point_bytes[start..end].chunks(data_size)
                });

                use PointCloud2DataType as T;

                let array: ArrayRef = if count == 1 {
                    match datatype {
                        T::I8 => make_array!(elem_iter, i8, Int8Array, is_be),
                        T::U8 => make_array!(elem_iter, u8, UInt8Array, is_be),
                        T::I16 => make_array!(elem_iter, i16, Int16Array, is_be),
                        T::U16 => make_array!(elem_iter, u16, UInt16Array, is_be),
                        T::I32 => make_array!(elem_iter, i32, Int32Array, is_be),
                        T::U32 => make_array!(elem_iter, u32, UInt32Array, is_be),
                        T::F32 => make_array!(elem_iter, f32, Float32Array, is_be),
                        T::F64 => make_array!(elem_iter, f64, Float64Array, is_be),
                    }
                } else {
                    match datatype {
                        T::I8 => {
                            make_list_array!(elem_iter, name, count, i8, DataType::Int8, is_be)
                        }
                        T::U8 => {
                            make_list_array!(elem_iter, name, count, u8, DataType::UInt8, is_be)
                        }
                        T::I16 => {
                            make_list_array!(elem_iter, name, count, i16, DataType::Int16, is_be)
                        }
                        T::U16 => {
                            make_list_array!(elem_iter, name, count, u16, DataType::UInt16, is_be)
                        }
                        T::I32 => {
                            make_list_array!(elem_iter, name, count, i32, DataType::Int32, is_be)
                        }
                        T::U32 => {
                            make_list_array!(elem_iter, name, count, u32, DataType::UInt32, is_be)
                        }
                        T::F32 => {
                            make_list_array!(elem_iter, name, count, f32, DataType::Float32, is_be)
                        }
                        T::F64 => {
                            make_list_array!(elem_iter, name, count, f64, DataType::Float64, is_be)
                        }
                    }
                };

                Ok((Arc::new(field), array))
            })
            .try_collect()?;

        let array = StructArray::from(columns);
        Ok(array)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FieldDesc {
    field: Field,
    datatype: PointCloud2DataType,
    offset: usize,
    data_size: usize,
    count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, FromPrimitive)]
#[repr(u8)]
enum PointCloud2DataType {
    I8 = 1,
    U8 = 2,
    I16 = 3,
    U16 = 4,
    I32 = 5,
    U32 = 6,
    F32 = 7,
    F64 = 8,
}

impl PointCloud2DataType {
    pub fn to_arrow_datatype(&self) -> DataType {
        match self {
            PointCloud2DataType::I8 => DataType::Int8,
            PointCloud2DataType::U8 => DataType::UInt8,
            PointCloud2DataType::I16 => DataType::Int16,
            PointCloud2DataType::U16 => DataType::UInt8,
            PointCloud2DataType::I32 => DataType::Int32,
            PointCloud2DataType::U32 => DataType::UInt32,
            PointCloud2DataType::F32 => DataType::Float32,
            PointCloud2DataType::F64 => DataType::Float64,
        }
    }
}
