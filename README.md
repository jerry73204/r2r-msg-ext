# Extensions for [r2r](https://docs.rs/r2r/) Message Types

This crate extends common ROS message types, provided by
[r2r](https://docs.rs/r2r/), with type conversion to data types from
third-party crates. The following crates are supported.

- [nalgebra](https://docs.rs/nalgebra/)
- [opencv](https://docs.rs/opencv/)
- [arrow](https://docs.rs/arrow/)


## Usage

Import this crate to your Cargo.toml. Enable `with-opencv` feature if
OpenCv support is desired. Other features include `with-nalgebra` and
`with-arrow`.

```toml
[dependencies.r2r-msg-ext]
git = "https://github.com/jerry73204/r2r-msg-ext.git"
rev = "0.1.0"
features = ["with-opencv"]
```

With this crate imported, you can get a new extended method
`Image::to_mat()` in your code for example.


```rust
use opencv::prelude::*;
use r2r::sensor_msgs::msg::Image;
use r2r_msg_ext::prelude::*;

let image: Image = get_image_from_subscriber();
let mat: Mat = image.to_mat()?;
```

## License

This software is distributed under MIT license. Please check the
[LICENSE.txt](LICENSE.txt) file.
