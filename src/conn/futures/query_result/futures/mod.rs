pub mod collect;
pub mod collect_all;
pub mod for_each;
pub mod map;
pub mod reduce;

pub use self::collect::{
    BinCollect,
    Collect,
    CollectNew,
    new as new_collect,
    new_new as new_collect_new,
    new_bin as new_bin_collect,
};

pub use self::collect_all::{
    CollectAll,
    CollectAllNew,
    new as new_collect_all,
    new_new as new_collect_all_new,
};

pub use self::for_each::{
    BinForEach,
    ForEach,
    ForEachNew,
    new as new_for_each,
    new_bin as new_bin_for_each,
    new_new as new_for_each_new,
};

pub use self::map::{
    BinMap,
    Map,
    MapNew,
    new as new_map,
    new_new as new_map_new,
    new_bin as new_bin_map,
};

pub use self::reduce::{
    BinReduce,
    Reduce,
    ReduceNew,
    new as new_reduce,
    new_bin as new_bin_reduce,
    new_new as new_reduce_new,
};
