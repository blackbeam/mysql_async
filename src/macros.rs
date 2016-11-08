// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

macro_rules! steps {
    ($fut:ty { $($step:ident($ty:ty),)+ }) => (
        enum Step {
            $($step($ty),)+
        }

        enum Out {
            $($step(<$ty as Future>::Item),)+
        }

        impl $fut {
            fn either_poll(&mut self) -> Result<Async<Out>> {
                match self.step {
                    $(Step::$step(ref mut fut) => Ok(Ready(Out::$step(try_ready!(fut.poll())))),)+
                }
            }
        }
    );
}

/// This macro allows you to pass named params to a prepared statement.
///
/// #### Example
/// ```ignore
/// conn.prep_exec("SELECT :param1, :param2, :param1", params! {
///     "param1" => "foo",
///     "param2" => "bar",
/// });
/// ```
#[macro_export]
macro_rules! params {
    ($($name:expr => $value:expr),*) => (
        vec![
            $((::std::string::String::from($name), $crate::Value::from($value))),*
        ]
    );
    ($($name:expr => $value:expr),*,) => (
        params!($($name => $value),*)
    );
}
