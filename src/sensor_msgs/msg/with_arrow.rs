use anyhow::{anyhow, bail, ensure, Result};
use arrow::{
    array::{
        Array, ArrayData, ArrayRef, AsArray, FixedSizeListArray, Float32Array, Float64Array,
        Int16Array, Int32Array, Int8Array, StructArray, UInt16Array, UInt32Array, UInt8Array,
    },
    buffer::Buffer,
    datatypes::{DataType, Field},
};
use itertools::{izip, Itertools};
use num_derive::FromPrimitive;
use num_traits::{FromPrimitive, ToBytes};
use r2r::{
    sensor_msgs::msg::{PointCloud2, PointField},
    std_msgs::msg::Header,
};
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

pub trait PointCloud2ArrowExt
where
    Self: Sized,
{
    fn to_arrow_array(&self) -> Result<StructArray>;
    fn from_arrow_array(header: Header, array: &StructArray) -> Result<Self>;
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

                let datatype = RosDataType::from_u8(datatype)
                    .ok_or_else(|| anyhow!("Unsupported datatype {datatype}"))?;
                let arrow_datatype = datatype.to_arrow_datatype();
                let size = datatype.size();
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

                use DataType as D;
                use RosDataType as T;

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
                            make_list_array!(elem_iter, name, count, i8, D::Int8, is_be)
                        }
                        T::U8 => {
                            make_list_array!(elem_iter, name, count, u8, D::UInt8, is_be)
                        }
                        T::I16 => {
                            make_list_array!(elem_iter, name, count, i16, D::Int16, is_be)
                        }
                        T::U16 => {
                            make_list_array!(elem_iter, name, count, u16, D::UInt16, is_be)
                        }
                        T::I32 => {
                            make_list_array!(elem_iter, name, count, i32, D::Int32, is_be)
                        }
                        T::U32 => {
                            make_list_array!(elem_iter, name, count, u32, D::UInt32, is_be)
                        }
                        T::F32 => {
                            make_list_array!(elem_iter, name, count, f32, D::Float32, is_be)
                        }
                        T::F64 => {
                            make_list_array!(elem_iter, name, count, f64, D::Float64, is_be)
                        }
                    }
                };

                Ok((Arc::new(field), array))
            })
            .try_collect()?;

        let array = StructArray::from(columns);
        Ok(array)
    }

    fn from_arrow_array(header: Header, array: &StructArray) -> Result<Self> {
        let is_be = cfg!(target_endian = "big");
        let mut offset = 0;

        let fields: Vec<_> = izip!(array.column_names(), array.columns())
            .map(|(name, col): (&str, _)| {
                let dt = col.data_type();

                let point_field = if let Some(ros_dt) = RosDataType::from_arrow_datatype(dt) {
                    let field = PointField {
                        name: name.to_string(),
                        offset: offset as u32,
                        datatype: ros_dt as u8,
                        count: 1,
                    };
                    offset += dt.size();
                    field
                } else {
                    let DataType::FixedSizeList(ref field_ty, count) = *dt else {
                        bail!("Unsupported data type {dt}",);
                    };
                    let field_dt = field_ty.data_type();
                    let Some(ros_dt) = RosDataType::from_arrow_datatype(field_dt) else {
                        bail!("Unsupported data type {field_dt}",);
                    };

                    let field = PointField {
                        name: name.to_string(),
                        offset: offset as u32,
                        datatype: ros_dt as u8,
                        count: count as u32,
                    };
                    offset += field_dt.size() * count as usize;
                    field
                };

                Ok(point_field)
            })
            .try_collect()?;

        let point_step = offset;
        let row_step = point_step;
        let height = array.len();
        let mut data = vec![0u8; height * row_step];

        izip!(array.columns(), &fields).for_each(|(col, field)| {
            let PointField { offset, count, .. } = *field;
            let arrow_data_type = col.data_type();
            let range = {
                let start = offset as usize;
                let end = start + arrow_data_type.size() * count as usize;
                start..end
            };

            let point_bytes_iter = data
                .chunks_mut(row_step)
                .map(move |row| &mut row[range.clone()]);

            macro_rules! write_column {
                ($iter:ident, $array:ident, $is_be:ident, $array_ty:ty) => {{
                    let array: &$array_ty = $array.as_primitive();

                    if $is_be {
                        izip!($iter, array.values()).for_each(|(bytes, val)| {
                            bytes.copy_from_slice(&val.to_be_bytes());
                        });
                    } else {
                        izip!($iter, array.values()).for_each(|(bytes, val)| {
                            bytes.copy_from_slice(&val.to_le_bytes());
                        });
                    }
                }};
            }

            macro_rules! write_list_column {
                ($iter:ident, $array:ident, $is_be:ident, $field:ident, $array_ty:ty) => {{
                    let array: &FixedSizeListArray = $array.as_fixed_size_list();
                    let elem_size = $field.data_type().size();

                    if $is_be {
                        izip!($iter, 0..array.len()).for_each(|(point_bytes, idx)| {
                            let list = array.value(idx);
                            let list: &$array_ty = list.as_primitive();

                            izip!(point_bytes.chunks_mut(elem_size), list.values()).for_each(
                                |(elem_bytes, val)| {
                                    elem_bytes.copy_from_slice(&val.to_be_bytes());
                                },
                            );
                        });
                    } else {
                        izip!($iter, 0..array.len()).for_each(|(point_bytes, idx)| {
                            let list = array.value(idx);
                            let list: &$array_ty = list.as_primitive();

                            izip!(point_bytes.chunks_mut(elem_size), list.values()).for_each(
                                |(elem_bytes, val)| {
                                    elem_bytes.copy_from_slice(&val.to_le_bytes());
                                },
                            );
                        });
                    }
                }};
            }

            match arrow_data_type {
                DataType::Int8 => {
                    write_column!(point_bytes_iter, col, is_be, Int8Array);
                }
                DataType::Int16 => {
                    write_column!(point_bytes_iter, col, is_be, Int16Array);
                }
                DataType::Int32 => {
                    write_column!(point_bytes_iter, col, is_be, Int32Array);
                }
                DataType::UInt8 => {
                    write_column!(point_bytes_iter, col, is_be, UInt8Array);
                }
                DataType::UInt16 => {
                    write_column!(point_bytes_iter, col, is_be, UInt16Array);
                }
                DataType::UInt32 => {
                    write_column!(point_bytes_iter, col, is_be, UInt32Array);
                }
                DataType::Float32 => {
                    write_column!(point_bytes_iter, col, is_be, Float32Array);
                }
                DataType::Float64 => {
                    write_column!(point_bytes_iter, col, is_be, Float64Array);
                }
                DataType::FixedSizeList(field, _) => {
                    let field_ty = field.data_type();

                    match field_ty {
                        DataType::Int8 => {
                            write_list_column!(point_bytes_iter, col, is_be, field, Int8Array);
                        }
                        DataType::Int16 => {
                            write_list_column!(point_bytes_iter, col, is_be, field, Int16Array);
                        }
                        DataType::Int32 => {
                            write_list_column!(point_bytes_iter, col, is_be, field, Int32Array);
                        }
                        DataType::UInt8 => {
                            write_list_column!(point_bytes_iter, col, is_be, field, UInt8Array);
                        }
                        DataType::UInt16 => {
                            write_list_column!(point_bytes_iter, col, is_be, field, UInt16Array);
                        }
                        DataType::UInt32 => {
                            write_list_column!(point_bytes_iter, col, is_be, field, UInt32Array);
                        }
                        DataType::Float32 => {
                            write_list_column!(point_bytes_iter, col, is_be, field, Float32Array);
                        }
                        DataType::Float64 => {
                            write_list_column!(point_bytes_iter, col, is_be, field, Float64Array);
                        }
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
        });

        Ok(PointCloud2 {
            header,
            height: height as u32,
            width: 1,
            fields,
            is_bigendian: is_be,
            point_step: point_step as u32,
            row_step: point_step as u32,
            data,
            is_dense: true,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FieldDesc {
    field: Field,
    datatype: RosDataType,
    offset: usize,
    data_size: usize,
    count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, FromPrimitive)]
#[repr(u8)]
enum RosDataType {
    I8 = 1,
    U8 = 2,
    I16 = 3,
    U16 = 4,
    I32 = 5,
    U32 = 6,
    F32 = 7,
    F64 = 8,
}

impl RosDataType {
    pub fn size(&self) -> usize {
        match self {
            RosDataType::I8 => 1,
            RosDataType::U8 => 1,
            RosDataType::I16 => 2,
            RosDataType::U16 => 2,
            RosDataType::I32 => 4,
            RosDataType::U32 => 4,
            RosDataType::F32 => 4,
            RosDataType::F64 => 8,
        }
    }

    pub fn to_arrow_datatype(&self) -> DataType {
        match self {
            RosDataType::I8 => DataType::Int8,
            RosDataType::U8 => DataType::UInt8,
            RosDataType::I16 => DataType::Int16,
            RosDataType::U16 => DataType::UInt8,
            RosDataType::I32 => DataType::Int32,
            RosDataType::U32 => DataType::UInt32,
            RosDataType::F32 => DataType::Float32,
            RosDataType::F64 => DataType::Float64,
        }
    }

    pub fn from_arrow_datatype(dt: &DataType) -> Option<Self> {
        Some(match dt {
            DataType::Int8 => Self::I8,
            DataType::UInt8 => Self::U8,
            DataType::Int16 => Self::I16,
            DataType::UInt16 => Self::U16,
            DataType::Int32 => Self::I32,
            DataType::UInt32 => Self::U32,
            DataType::Float32 => Self::F32,
            DataType::Float64 => Self::F64,
            _ => return None,
        })
    }
}
