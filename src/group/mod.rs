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

use crate::eraftpb::Message;
use std::collections::hash_map::Iter;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

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

#[derive(Clone, Debug)]
pub(crate) struct DelegatedMessage {
    pub delegated_peers: HashSet<u64>,
    pub inner: Message,
}

impl DelegatedMessage {
    fn into_message(mut self) -> Message {
        let targets = self.delegated_peers.drain().collect::<Vec<_>>();
        let mut m = self.inner;
        m.bcast_targets = targets;
        m
    }
}

/// Maintain all the groups info in Follower Replication
///
/// # Notice
///
/// A node only belongs to one group
///
#[derive(Debug, Clone)]
pub struct Groups {
    /// Group config metadata
    pub meta: GroupsConfig,
    // The messages to be sent to any delegates
    // The key is a group ID
    delegated_msgs: HashMap<u64, Vec<DelegatedMessage>>,
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

    /// Take all the delegated messages into a vec
    #[inline]
    pub fn take_messages(&mut self) -> Vec<Message> {
        self.delegated_msgs
            .drain()
            .map(|(_, mut msgs)| msgs.drain(..).map(|m| m.into_message()).collect::<Vec<_>>())
            .flatten()
            .collect::<Vec<_>>()
    }

    /// Create a DelegatedMessage and push it to the corresponding `delegated_msgs`
    pub(crate) fn insert_delegated_msg(&mut self, group_id: u64, m: Message, peer: Option<u64>) {
        let mut set = HashSet::default();
        if let Some(p) = peer {
            set.insert(p);
        }
        let msg = DelegatedMessage {
            delegated_peers: set,
            inner: m,
        };
        match self.delegated_msgs.get_mut(&group_id) {
            Some(msgs) => {
                msgs.push(msg);
            }
            None => {
                self.delegated_msgs.insert(group_id, vec![msg]);
            }
        }
    }

    /// Get the last delegated message
    #[inline]
    pub(crate) fn get_latest_delegated_msg(
        &mut self,
        group_id: u64,
    ) -> Option<&mut DelegatedMessage> {
        self.delegated_msgs
            .get_mut(&group_id)
            .and_then(|msgs| msgs.last_mut())
    }

    /// Get group members by the member id
    #[inline]
    pub fn get_members(&self, member: u64) -> Option<Vec<u64>> {
        self.indexes
            .get(&member)
            .and_then(|group_id| self.get_members_by_group(*group_id))
    }

    /// Get group members by group id
    #[inline]
    pub fn get_members_by_group(&self, group_id: u64) -> Option<Vec<u64>> {
        self.meta.inner.get(&group_id).cloned()
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
            delegated_msgs: HashMap::new(),
        }
    }
}
