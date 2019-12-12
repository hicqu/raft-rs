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

use std::collections::{hash_map::Entry as MapEntry, HashMap};

use crate::progress::progress_set::ProgressSet;
use crate::raft::INVALID_ID;

/// Maintain all the groups info in Follower Replication
///
/// # Notice
///
/// A node only belongs to one group
///
#[derive(Debug, Clone, Default)]
pub struct Groups {
    // node id => (group id, delegate id).
    indexes: HashMap<u64, (u64, u64)>,

    // Use to construct `bcast_targets` for delegates quickly.
    bcast_targets: HashMap<u64, Vec<u64>>,

    // Peers without chosen delegates.
    unresolved: Vec<u64>,

    leader_group_id: u64,
}

impl<'a> Groups {
    /// Create new Groups with given configuration.
    pub(crate) fn new(config: Vec<(u64, Vec<u64>)>) -> Self {
        let mut indexes = HashMap::new();
        let mut unresolved = Vec::new();
        for (group_id, members) in config {
            for id in members {
                indexes.insert(id, (group_id, INVALID_ID));
                unresolved.push(id);
            }
        }

        Self {
            indexes,
            unresolved,
            ..Default::default()
        }
    }

    pub(crate) fn set_leader_group_id(&mut self, leader_group: u64) {
        self.leader_group_id = leader_group;
    }

    /// Get group id by member id.
    pub(crate) fn get_group_id(&self, member: u64) -> Option<u64> {
        self.indexes.get(&member).map(|(gid, _)| *gid)
    }

    /// Get a delegate for `to`. The return value could be `to` itself.
    pub fn get_delegate(&self, to: u64) -> u64 {
        match self.indexes.get(&to) {
            Some((_, delegate)) => *delegate,
            None => INVALID_ID,
        }
    }

    // Pick a delegate for the given peer.
    //
    // The delegate must satisfy conditions below:
    // 1. The progress state should be `ProgressState::Replicate`;
    // 2. The progress has biggest `match`;
    // If all the members are requiring snapshots, use given `to`.
    fn pick_delegate(&mut self, to: u64, prs: &ProgressSet) {
        let group_id = match self.indexes.get(&to) {
            Some((_, delegate)) if *delegate != INVALID_ID => return,
            Some((gid, _)) if *gid == self.leader_group_id => return,
            Some((gid, _)) => *gid,
            None => return,
        };

        println!("pick delegate for {}", to);
        let (mut chosen, mut matched, mut bcast_targets) = (INVALID_ID, 0, vec![]);
        for id in self.candidate_delegates(group_id) {
            let pr = prs.get(id).unwrap();
            if matched < pr.matched {
                if chosen != INVALID_ID {
                    bcast_targets.push(chosen);
                }
                chosen = id;
                matched = pr.matched;
            } else {
                bcast_targets.push(id);
            }
        }
        println!(
            "pick delegate for {} choose {}, others: {:?}",
            to, chosen, bcast_targets
        );
        if chosen != INVALID_ID && !bcast_targets.is_empty() {
            let (_, d) = self.indexes.get_mut(&chosen).unwrap();
            *d = chosen;
            for id in &bcast_targets {
                let (_, d) = self.indexes.get_mut(id).unwrap();
                *d = chosen;
            }
            self.bcast_targets.insert(chosen, bcast_targets);
        }
    }

    fn candidate_delegates(&'a self, group_id: u64) -> impl Iterator<Item = u64> + 'a {
        self.indexes.iter().filter_map(move |(peer, (gid, _))| {
            if group_id == *gid {
                return Some(*peer);
            }
            None
        })
    }

    /// Unset the delegate by delegate id.
    pub(crate) fn remove_delegate(&mut self, delegate: u64) {
        if self.indexes.remove(&delegate).is_some() {
            for (peer, (_, d)) in self.indexes.iter_mut() {
                if *d == delegate {
                    *d = INVALID_ID;
                    self.unresolved.push(*peer);
                }
            }
            self.bcast_targets.remove(&delegate);
        }
    }

    pub(crate) fn is_delegated(&self, to: u64) -> bool {
        self.indexes
            .get(&to)
            .map_or(false, |x| x.1 != INVALID_ID && x.1 != to)
    }

    pub(crate) fn get_bcast_targets(&self, delegate: u64) -> Option<&Vec<u64>> {
        debug_assert!(self.unresolved.is_empty());
        self.bcast_targets.get(&delegate)
    }

    /// Update given `peer`'s group ID. Return `true` if any peers are unresolved.
    pub(crate) fn update_group_id(&mut self, peer: u64, group_id: u64) -> bool {
        let mut remove_delegate = false;
        match self.indexes.entry(peer) {
            MapEntry::Occupied(e) => {
                if group_id == INVALID_ID {
                    let (_, (_, d)) = e.remove_entry();
                    remove_delegate = d == peer;
                } else {
                    let (ref mut gid, ref mut d) = e.into_mut();
                    if *gid != group_id {
                        *gid = group_id;
                        *d = INVALID_ID;
                        self.unresolved.push(peer);
                        remove_delegate = *d == peer;
                    }
                }
            }
            MapEntry::Vacant(e) => {
                e.insert((group_id, INVALID_ID));
                self.unresolved.push(peer);
            }
        }
        if remove_delegate {
            self.remove_delegate(peer);
        }
        !self.unresolved.is_empty()
    }

    // Pick delegates for all peers if need.
    // TODO: change to `pub(crate)` after we find a simple way to test.
    pub fn resolve_delegates(&mut self, prs: &ProgressSet) {
        if !self.unresolved.is_empty() {
            for peer in std::mem::replace(&mut self.unresolved, vec![]) {
                self.pick_delegate(peer, prs);
            }
        }
    }

    pub(crate) fn check_pr_active(&mut self, id: u64, active: bool) {
        if !active {
            self.remove_delegate(id);
        }
    }
}
