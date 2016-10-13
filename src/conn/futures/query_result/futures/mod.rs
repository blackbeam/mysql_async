mod collect;
mod collect_all;
mod for_each;
mod map;
mod reduce;

pub use self::collect::{
    BinCollect,
    Collect,
    new as new_collect,
    new_bin as new_bin_collect,
};

pub use self::collect_all::{
    CollectAll,
    new as new_collect_all,
};

pub use self::for_each::{
    BinForEach,
    ForEach,
    new as new_for_each,
    new_bin as new_bin_for_each,
};

pub use self::map::{
    BinMap,
    Map,
    new as new_map,
    new_bin as new_bin_map,
};

pub use self::reduce::{
    BinReduce,
    Reduce,
    new as new_reduce,
    new_bin as new_bin_reduce,
};
