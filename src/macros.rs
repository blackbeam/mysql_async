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
