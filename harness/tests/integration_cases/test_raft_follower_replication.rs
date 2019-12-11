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
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::test_util::*;
use harness::{Interface, Network};
use raft::eraftpb::*;
use raft::storage::MemStorage;
use raft::*;
use rand::Rng;
use slog::Logger;
use std::collections::HashSet;
use std::iter::FromIterator;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum FollowerScenario {
    // Follower is ready for new raft logs
    UpToDate,
    // Follower's next_idx = given - 1 and matched = given - 2
    NeedEntries(u64),
    // Follower need snapshot
    Snapshot,
}

// Sandbox is a helper struct to represent a determined stable state of a raft cluster.
struct Sandbox {
    leader: u64,
    // initialized last index
    last_index: u64,
    followers: Vec<(u64, FollowerScenario)>,
    network: Network,
}

impl Sandbox {
    // Create a sandbox for testing
    //
    // The relationship between followers in different states:
    //
    //  +-----+
    //     |
    //     |
    //     |  Follower::Snapshot
    //     |
    //     |
    //  +-----+ <snapshot_index>
    //     |
    //     |
    //     |
    //     |  Follower::NeedEntries
    //     |
    //     |
    //     |
    //  +-----+ <last_index> Follower::UpToDate
    //
    // The given `leader` and `followers` should be mutually exclusive.
    // The ProgressSet in generated followers are uninitialized
    //
    pub fn new(
        l: &Logger,
        leader: u64,
        followers: Vec<(u64, FollowerScenario)>,
        group_config: Vec<(u64, Vec<u64>)>,
        snapshot_index: u64,
        last_index: u64,
    ) -> Self {
        if snapshot_index >= last_index {
            panic!(
                "snapshot_index {} should be less than last_index {}",
                snapshot_index, last_index
            );
        }
        if last_index < 2 {
            panic!("last_index {} should be larger than 1", last_index);
        }
        let peers = followers.iter().map(|(id, _)| *id).collect::<HashSet<_>>();
        if peers.contains(&leader) {
            panic!(
                "followers {:?} and leader {} should be mutually exclusive",
                &peers, leader
            )
        }
        let mut peers = peers.into_iter().collect::<Vec<_>>();
        peers.push(leader);
        let c = new_test_config(leader, 10, 1);
        let storage = new_storage(peers.clone(), snapshot_index, last_index - 1);
        let mut leader_node = Interface::new(Raft::new(&c, storage, l).unwrap());
        leader_node.set_groups(group_config);
        leader_node.become_candidate();
        leader_node.become_leader();
        let entries = leader_node.raft_log.all_entries();
        let mut interfaces = followers
            .clone()
            .drain(..)
            .map(|(id, scenario)| {
                let storage =
                    new_storage_by_scenario(scenario, peers.clone(), snapshot_index, last_index);
                let mut c = c.clone();
                c.id = id;
                let node = Interface::new(Raft::new(&c, storage, l).unwrap());
                let node_entries = node.raft_log.all_entries();
                if scenario != FollowerScenario::Snapshot {
                    Self::assert_entries_consistent(entries.clone(), node_entries);
                }
                let mut pr = leader_node.mut_prs().get_mut(id).unwrap();
                pr.state = match scenario {
                    FollowerScenario::NeedEntries(_) | FollowerScenario::UpToDate => {
                        ProgressState::Replicate
                    }
                    FollowerScenario::Snapshot => ProgressState::Probe,
                };
                pr.paused = false;
                pr.recent_active = true;
                pr.matched = node.raft_log.last_index();
                pr.next_idx = node.raft_log.last_index() + 1;
                Some(node)
            })
            .collect::<Vec<Option<Interface>>>();
        interfaces.insert(0, Some(leader_node));
        let network = Network::new(interfaces, l);
        Self {
            leader,
            last_index,
            followers,
            network,
        }
    }

    // Only for `UpToDate` and `NeedEntries`
    #[inline]
    fn assert_entries_consistent(leader: Vec<Entry>, target: Vec<Entry>) {
        for (e1, e2) in leader.iter().zip(target) {
            assert_eq!(e1.index, e2.index);
            assert_eq!(e1.term, e2.term);
        }
    }

    #[inline]
    fn assert_final_state(&self) {
        self.network.peers.iter().for_each(|(id, n)| {
            assert_eq!(
                n.raft_log.last_index(),
                self.last_index + 1,
                "The peer {} last index should be up-to-date",
                id
            )
        });
    }

    // Get mutable Interface of the leader
    #[inline]
    pub fn leader_mut(&mut self) -> &mut Interface {
        let leader = self.leader;
        self.network.peers.get_mut(&leader).unwrap()
    }

    // Get immutable Interface of the leader
    #[inline]
    pub fn leader(&self) -> &Interface {
        let leader = self.leader;
        self.network.peers.get(&leader).unwrap()
    }

    // Get a mutable Interface by given id
    #[inline]
    pub fn get_mut(&mut self, id: u64) -> &mut Interface {
        self.network.peers.get_mut(&id).unwrap()
    }
}

fn new_storage(peers: Vec<u64>, snapshot_index: u64, last_index: u64) -> MemStorage {
    let s = MemStorage::new_with_conf_state((peers.clone(), vec![]));
    let snapshot = new_snapshot(snapshot_index, 1, peers.clone());
    s.wl().apply_snapshot(snapshot).expect("");
    if snapshot_index < last_index {
        let mut ents = vec![];
        for index in snapshot_index + 1..=last_index {
            ents.push(empty_entry(1, index));
        }
        s.wl().append(&ents).expect("");
    }
    s
}

fn new_storage_by_scenario(
    scenario: FollowerScenario,
    peers: Vec<u64>,
    snapshot_index: u64,
    last_index: u64,
) -> MemStorage {
    let s = MemStorage::new_with_conf_state((peers.clone(), vec![]));
    match scenario {
        FollowerScenario::UpToDate => {
            let snapshot = new_snapshot(snapshot_index, 1, peers.clone());
            s.wl().apply_snapshot(snapshot).expect("");
            let mut ents = vec![];
            for index in snapshot_index + 1..last_index {
                ents.push(empty_entry(1, index));
            }
            ents.push(empty_entry(2, last_index));
            s.wl().append(&ents).expect("");
        }
        FollowerScenario::NeedEntries(index) => {
            let snapshot = new_snapshot(snapshot_index, 1, peers.clone());
            s.wl().apply_snapshot(snapshot).expect("");
            let li = if index <= last_index && index > snapshot_index {
                index
            } else {
                rand::thread_rng().gen_range(snapshot_index + 1, last_index)
            };
            let mut ents = vec![];
            for index in snapshot_index + 1..li {
                ents.push(empty_entry(1, index));
            }
            if li == last_index {
                ents.push(empty_entry(2, index));
            }
            s.wl().append(&ents).expect("");
        }
        FollowerScenario::Snapshot => {
            let li = rand::thread_rng().gen_range(1, snapshot_index);
            let mut ents = vec![];
            for index in 2..li {
                ents.push(empty_entry(1, index))
            }
            s.wl().append(&ents).expect("");
        }
    };
    s
}

// test_pick_delegate ensures that the delegate should be able to send entries to the other group
// members in leader's view.
#[test]
fn test_pick_group_delegate() {
    let l = default_logger();
    let group_config = vec![(2, vec![1]), (1, vec![2, 3, 4])];
    let tests = vec![
        (
            vec![4],
            MessageType::MsgAppend,
            vec![
                (2, FollowerScenario::NeedEntries(6)),
                (3, FollowerScenario::NeedEntries(7)),
                (4, FollowerScenario::NeedEntries(8)),
            ],
        ),
        (
            vec![2, 3, 4],
            MessageType::MsgSnapshot,
            vec![
                (2, FollowerScenario::Snapshot),
                (3, FollowerScenario::Snapshot),
                (4, FollowerScenario::Snapshot),
            ],
        ),
        (
            vec![2],
            MessageType::MsgAppend,
            vec![
                (2, FollowerScenario::UpToDate),
                (3, FollowerScenario::Snapshot),
                (4, FollowerScenario::NeedEntries(7)),
            ],
        ),
    ];
    for (expected_delegate, expected_msg_type, input) in tests {
        let mut sandbox = Sandbox::new(&l, 1, input.clone(), group_config.clone(), 5, 10);
        sandbox
            .network
            .dispatch(vec![new_message(1, 1, MessageType::MsgPropose, 1)])
            .expect("");
        let mut msgs = sandbox.leader_mut().read_messages();
        assert_eq!(1, msgs.len(), "Should only send one msg: {:?}", input);
        let m = msgs.pop().unwrap();
        assert_eq!(
            m.msg_type, expected_msg_type,
            "The sent msg type should be {:?} but got {:?}",
            expected_delegate, m.msg_type,
        );
        let delegate = m.to;
        let delegate_set: HashSet<u64> = HashSet::from_iter(expected_delegate);
        assert!(
            delegate_set.contains(&delegate),
            "set {:?}, delegate {}",
            &delegate_set,
            delegate
        );
        assert_eq!(
            sandbox.leader().groups().get_delegate(1),
            Some(delegate),
            "The picked delegate should be cached"
        );
    }
}

// test_delegate_in_group_containing_leader ensures that the leader send msgs directly to the followers in the same group
#[test]
fn test_delegate_in_group_containing_leader() {
    let l = default_logger();
    let group_config = vec![(1, vec![1, 2, 3, 4])];
    let followers = vec![
        (2, FollowerScenario::NeedEntries(7)),
        (3, FollowerScenario::Snapshot),
        (4, FollowerScenario::UpToDate),
    ];
    let mut sandbox = Sandbox::new(&l, 1, followers.clone(), group_config.clone(), 5, 10);
    sandbox
        .network
        .dispatch(vec![new_message(1, 1, MessageType::MsgPropose, 1)])
        .expect("");
    let msgs = sandbox.leader_mut().read_messages();
    assert_eq!(msgs.len(), 3);
    msgs.iter()
        .for_each(|m| assert!(m.bcast_targets.is_empty()));
}

#[test]
fn test_broadcast_append_use_delegate() {
    let l = default_logger();
    let mut sandbox = Sandbox::new(
        &l,
        1,
        vec![
            (2, FollowerScenario::NeedEntries(8)),
            (3, FollowerScenario::NeedEntries(7)),
            (4, FollowerScenario::NeedEntries(6)),
        ],
        vec![(2, vec![1]), (1, vec![2, 3, 4])],
        5,
        10,
    );
    sandbox
        .network
        .dispatch(vec![new_message(1, 1, MessageType::MsgPropose, 1)])
        .expect("");
    let mut msgs = sandbox.leader_mut().read_messages();
    assert_eq!(1, msgs.len());
    let m = msgs.pop().unwrap();
    assert_eq!(m.msg_type, MessageType::MsgAppend);
    assert!(m.bcast_targets.contains(&3));
    assert!(m.bcast_targets.contains(&4));
    let delegate = m.to;
    assert_eq!(delegate, 2);
    sandbox.network.dispatch(vec![m]).expect("");
    assert_eq!(Some(2), sandbox.leader().groups().get_delegate(1));
    let mut msgs = sandbox.get_mut(delegate).read_messages();
    assert_eq!(3, msgs.len());
    let bcast_resp = msgs.remove(0); // Send to leader first
    assert_eq!(bcast_resp.msg_type, MessageType::MsgAppendResponse);
    let to_send_ids = sandbox
        .followers
        .iter()
        .filter(|(id, _)| *id != delegate)
        .map(|(id, _)| *id)
        .collect::<Vec<u64>>();
    let set: HashSet<u64> = HashSet::from_iter(to_send_ids);
    msgs.iter().for_each(|m| {
        assert_eq!(
            m.from, 1,
            "the delegated message must looks like coming from leader"
        );
        assert_eq!(m.delegate, 2, "'delegate' must be set");
        assert_eq!(m.msg_type, MessageType::MsgAppend);
        assert!(set.contains(&m.to));
    });
    sandbox.network.send(vec![bcast_resp]);
    sandbox.network.send(msgs);
    sandbox.assert_final_state();
}

// test_no_delegate_in_group_containing_leader ensures that the picked delegate rejects broadcast
// request when its raft logs are not consistent with the leader
#[test]
fn test_delegate_reject_broadcast() {
    let l = default_logger();
    let group_config = vec![(2, vec![1]), (1, vec![2, 3, 4])];
    let followers = vec![
        (2, FollowerScenario::NeedEntries(7)),
        (3, FollowerScenario::Snapshot),
        (4, FollowerScenario::NeedEntries(12)),
    ];
    let mut sandbox = Sandbox::new(&l, 1, followers, group_config, 5, 20);
    sandbox.leader_mut().mut_prs().get_mut(4).unwrap().next_idx = 15; // make a conflict next_idx
    sandbox
        .network
        .dispatch(vec![new_message(1, 1, MessageType::MsgPropose, 1)])
        .expect("");
    let mut msgs = sandbox.leader_mut().read_messages();
    let m = msgs.pop().unwrap();
    assert_eq!(4, m.to);
    sandbox.network.dispatch(vec![m]).expect("");
    let mut msgs = sandbox.get_mut(4).read_messages();
    assert_eq!(1, msgs.len());
    let m = msgs.pop().unwrap();
    assert_eq!(MessageType::MsgAppendResponse, m.msg_type);
    assert!(m.reject);
    assert_eq!(1, m.to);
    assert_eq!(
        2,
        m.get_bcast_targets().len(),
        "If a delegate rejects broadcasting, it should send back all the `bcast_targets` to the leader"
    );
    sandbox.network.dispatch(vec![m]).expect("");
    assert_eq!(
        Some(4),
        sandbox.leader().groups().get_delegate(1),
        "The delegate won't be dismissed when rejecting MsgAppend"
    );
    let mut msgs = sandbox.leader_mut().read_messages();
    assert_eq!(1, msgs.len());
    let m = msgs.pop().unwrap();
    assert_eq!(4, m.to);
    assert_eq!(2, m.get_bcast_targets().len());
    sandbox.network.send(vec![m]);
    sandbox.assert_final_state();
}

#[test]
fn test_follower_only_send_reject_to_delegate() {
    let l = default_logger();
    let group_config = vec![(2, vec![1]), (1, vec![2, 3])];
    let followers = vec![
        (2, FollowerScenario::NeedEntries(10)),
        (3, FollowerScenario::NeedEntries(7)),
    ];
    let mut sandbox = Sandbox::new(&l, 1, followers, group_config, 5, 20);
    sandbox
        .network
        .dispatch(vec![new_message(1, 1, MessageType::MsgPropose, 1)])
        .expect("");
    let msgs = sandbox.leader_mut().read_messages();
    // Pick peer 2 as the delegate
    assert_eq!(Some(2), sandbox.leader().groups().get_delegate(1));
    sandbox.network.dispatch(msgs).expect("");
    let mut msgs = sandbox.get_mut(2).read_messages();
    // MsgAppendResponse to 1 and MsgAppend to 3
    // We only care about the latter
    assert_eq!(msgs.len(), 2);
    let m = msgs.remove(1);
    assert_eq!(m.get_msg_type(), MessageType::MsgAppend);
    assert_eq!(m.to, 3);
    assert_eq!(m.from, 1);
    assert_eq!(m.delegate, 2);
    sandbox.network.dispatch(vec![m]).expect("");
    let mut msgs = sandbox.get_mut(3).read_messages();
    assert_eq!(msgs.len(), 1);
    let m = msgs.pop().unwrap();
    assert_eq!(m.to, 2);
    assert!(m.reject);
}

#[test]
fn test_send_empty_msg_when_paused() {
    // TODO
}

#[test]
fn test_dismiss_delegate_when_not_active() {
    // TODO
}

#[test]
fn test_update_group_by_group_id_in_message() {
    // TODO
}
