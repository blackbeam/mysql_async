// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use sha1;

pub fn scramble(scr1: &[u8], scr2: Option<&[u8]>, password: &[u8]) -> Option<[u8; 20]> {
    if password.len() == 0 {
        return None;
    }

    let mut sha = sha1::Sha1::new();
    sha.update(password);
    let sha = sha.digest().bytes();

    let mut sha2 = sha1::Sha1::new();
    sha2.update(&sha[..]);
    let sha2 = sha2.digest().bytes();

    let mut hash = sha1::Sha1::new();
    hash.update(scr1);
    if let Some(scr2) = scr2 {
        hash.update(scr2);
    }
    hash.update(&sha2[..]);
    let hash = hash.digest().bytes();

    let mut output = [0u8; 20];

    for i in 0..20 {
        output[i] = sha[i] ^ hash[i];
    }

    Some(output)
}
