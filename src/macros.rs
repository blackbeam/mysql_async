#[macro_export]
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
