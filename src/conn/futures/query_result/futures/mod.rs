mod collect;
mod collect_all;
mod for_each;
mod map;
mod reduce;

pub use self::collect::Collect;
pub use self::collect::new_new as new_collect;
pub use self::collect_all::CollectAll;
pub use self::collect_all::new_new as new_collect_all;
pub use self::for_each::ForEach;
pub use self::for_each::new_new as new_for_each;
pub use self::map::Map;
pub use self::map::new_new as new_map;
pub use self::reduce::Reduce;
pub use self::reduce::new_new as new_reduce;