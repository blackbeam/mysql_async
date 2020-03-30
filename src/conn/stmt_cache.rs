// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use twox_hash::XxHash;

#[cfg(test)]
use std::collections::vec_deque::Iter;
use std::{
    borrow::Borrow,
    collections::{HashMap, VecDeque},
    hash::{BuildHasherDefault, Hash},
};

use crate::queryable::stmt::InnerStmt;

#[derive(Debug)]
pub struct StmtCache {
    cap: usize,
    map: HashMap<String, InnerStmt, BuildHasherDefault<XxHash>>,
    order: VecDeque<String>,
}

impl StmtCache {
    pub(crate) fn new(cap: usize) -> StmtCache {
        StmtCache {
            cap,
            map: Default::default(),
            order: VecDeque::with_capacity(cap),
        }
    }

    pub(crate) fn get<T>(&mut self, key: &T) -> Option<&InnerStmt>
    where
        String: Borrow<T>,
        String: PartialEq<T>,
        T: Hash + Eq,
        T: ?Sized,
    {
        if self.map.contains_key(key) {
            if let Some(pos) = self.order.iter().position(|x| x == key) {
                if let Some(inner_st) = self.order.remove(pos) {
                    self.order.push_back(inner_st);
                }
            }
            self.map.get(key)
        } else {
            None
        }
    }

    pub(crate) fn put(&mut self, key: String, value: InnerStmt) -> Option<InnerStmt> {
        self.map.insert(key.clone(), value);
        self.order.push_back(key);
        if self.order.len() > self.cap {
            self.order
                .pop_front()
                .and_then(|stmt| self.map.remove(&stmt))
        } else {
            None
        }
    }

    pub(crate) fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
    }

    #[cfg(test)]
    pub(crate) fn iter<'a>(&'a self) -> Iter<'a, String> {
        self.order.iter()
    }
}
