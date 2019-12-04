// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! The module includes the definition of new feature 'raft group' which is used for
//! **Follower Replication**
//!
//! # Follower Replication
//! See https://github.com/tikv/rfcs/pull/33

use std::collections::hash_map::Iter;
use std::collections::HashMap;
use std::iter::FromIterator;

/// The option used for feature [Follower Replication](https://github.com/tikv/rfcs/pull/33/files)
#[derive(Clone, Debug)]
pub struct FollowerReplicationOption {
    /// The leader choose a follower in a raft group as a delegate and ask it to send entries to
    /// the rest group members.
    /// If there is no group configured, no delegate will be picked
    pub follower_delegate: bool,

    /// The raft group definition. See [`GroupsConfig`](group/struct.GroupsConfig.html) for detail.
    pub groups: GroupsConfig,

    /// The max number of members of a group of which contains leader and the leader never pick a delegate.
    /// If the group size is larger than this, a delegate will be picked even the leader belongs to this group.
    pub max_leader_group_no_delegate: usize,
}

impl Default for FollowerReplicationOption {
    fn default() -> Self {
        FollowerReplicationOption {
            follower_delegate: false,
            groups: GroupsConfig::default(),
            max_leader_group_no_delegate: 5,
        }
    }
}

/// Configuration for distribution of raft nodes in groups.
/// For the inner hashmap, the key is group ID and value is the group members.
#[derive(Clone, Debug)]
pub struct GroupsConfig {
    inner: HashMap<u64, Vec<u64>>,
}

impl GroupsConfig {
    /// Create a new GroupsConfig
    pub fn new(config: Vec<(u64, Vec<u64>)>) -> Self {
        let inner = HashMap::from_iter(config.into_iter());
        Self { inner }
    }

    /// Return a iterator with inner group ID - group members pairs
    #[inline]
    pub fn iter(&self) -> Iter<'_, u64, Vec<u64>> {
        self.inner.iter()
    }
}

impl Default for GroupsConfig {
    fn default() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
}

/// Raft group configuration
///
/// # Notice
///
/// A node only belongs to one group
///
#[derive(Debug, Clone)]
pub struct Groups {
    /// Group config metadata
    pub meta: GroupsConfig,
    // node id => group id
    indexes: HashMap<u64, u64>,
    // group id => delegate id
    delegate_cache: HashMap<u64, u64>,
}

impl Groups {
    /// Create new Groups with given configuration
    pub fn new(meta: GroupsConfig) -> Self {
        let mut indexes = HashMap::new();
        for (group_id, members) in meta.inner.iter() {
            for member in members.iter() {
                indexes.insert(*member, *group_id);
            }
        }
        Self {
            meta,
            indexes,
            ..Default::default()
        }
    }

    /// Get group members by the member id
    #[inline]
    pub fn get_members(&self, member: u64) -> Option<&Vec<u64>> {
        self.indexes
            .get(&member)
            .and_then(|group_id| self.get_members_by_group(*group_id))
    }

    /// Get group members by group id
    #[inline]
    pub fn get_members_by_group(&self, group_id: u64) -> Option<&Vec<u64>> {
        self.meta.inner.get(&group_id)
    }

    /// Get group id by member id
    #[inline]
    pub fn get_group_id(&self, member: u64) -> Option<u64> {
        self.indexes.get(&member).cloned()
    }

    /// Set delegate for a group if the given delegate is a group member.
    #[inline]
    pub fn set_delegate(&mut self, delegate: u64) {
        if let Some(group) = self.get_group_id(delegate) {
            self.delegate_cache.insert(group, delegate);
        }
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

    /// Return the delegate for a group by node id
    #[inline]
    pub fn get_delegate_by_member(&self, member: u64) -> Option<u64> {
        self.get_group_id(member)
            .and_then(|group| self.get_delegate(group))
    }

    /// Whether the two nodes are in the same group
    #[inline]
    pub fn in_same_group(&self, a: u64, b: u64) -> bool {
        let ga = self.get_group_id(a);
        let gb = self.get_group_id(b);
        ga.is_some() && ga == gb
    }
}

impl Default for Groups {
    fn default() -> Self {
        Self {
            meta: Default::default(),
            indexes: HashMap::new(),
            delegate_cache: HashMap::new(),
        }
    }
}
