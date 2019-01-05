// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

macro_rules! const_assert {
    ($name:ident, $($xs:expr),+ $(,)*) => {
        #[allow(unknown_lints, eq_op)]
        const $name: [(); 0 - !($($xs)&&+) as usize] = [];
    };
}

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
                    $(Step::$step(ref mut fut) => Ok(Ready(Out::$step(::futures::try_ready!(fut.poll())))),)+
                }
            }
        }
    );
}
