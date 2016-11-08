// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

mod collect;
mod collect_all;
mod drop_result;
mod for_each;
mod map;
mod reduce;

pub use self::collect::Collect;
pub use self::collect::new_new as new_collect;
pub use self::collect_all::CollectAll;
pub use self::collect_all::new_new as new_collect_all;
pub use self::drop_result::DropResult;
pub use self::drop_result::new as new_drop_result;
pub use self::for_each::ForEach;
pub use self::for_each::new_new as new_for_each;
pub use self::map::Map;
pub use self::map::new_new as new_map;
pub use self::reduce::Reduce;
pub use self::reduce::new_new as new_reduce;