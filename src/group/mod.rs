// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The module includes the definition of new feature 'raft group' which is used for
//! **Follower Replication**
//!
//! # Follower Replication
//! See https://github.com/tikv/rfcs/pull/33

use std::collections::HashMap;

/// Maintain all the groups info in Follower Replication
///
/// # Notice
///
/// A node only belongs to one group
///
#[derive(Debug, Clone, Default)]
pub struct Groups {
    // node id => group id
    indexes: HashMap<u64, u64>,

    // group id => delegate id
    delegate_cache: HashMap<u64, u64>,
}

impl Groups {
    /// Create new Groups with given configuration
    pub fn new(config: Vec<(u64, Vec<u64>)>) -> Self {
        let mut indexes = HashMap::new();
        for (group_id, members) in config {
            for id in members {
                indexes.insert(id, group_id);
            }
        }

        Self {
            indexes,
            ..Default::default()
        }
    }

    /// Get group members by group id.
    #[inline]
    pub fn get_members(&self, group_id: u64) -> Vec<u64> {
        self.indexes
            .iter()
            .filter_map(
                |(node, gid)| {
                    if *gid == group_id {
                        Some(*node)
                    } else {
                        None
                    }
                },
            )
            .collect::<Vec<_>>()
    }

    /// Get group id by member id
    #[inline]
    pub fn get_group_id(&self, member: u64) -> Option<u64> {
        self.indexes.get(&member).cloned()
    }

    /// Set delegate for a group. The delegate must be in the group.
    #[inline]
    pub fn set_delegate(&mut self, group_id: u64, delegate: u64) {
        self.delegate_cache.insert(group_id, delegate);
    }

    /// Unset the delegate by delegate id.
    #[inline]
    pub fn remove_delegate(&mut self, delegate: u64) {
        if let Some(group_id) = self.get_group_id(delegate) {
            match self.delegate_cache.get(&group_id) {
                Some(d) if *d == delegate => {
                    self.delegate_cache.remove(&group_id);
                }
                _ => {}
            };
        }
    }

    /// Return the delegate for a group by group id
    #[inline]
    pub fn get_delegate(&self, group: u64) -> Option<u64> {
        self.delegate_cache.get(&group).cloned()
    }

    /// Update given `peer`'s group ID
    pub fn update_group_id(&mut self, peer: u64, group_id: u64) {
        let _ = self.indexes.insert(peer, group_id);
    }
}
