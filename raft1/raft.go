package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"github.com/shrtyk/raft/labgob"

	"github.com/shrtyk/raft/labrpc"
	"github.com/shrtyk/raft/raftapi"
	tester "github.com/shrtyk/raft/tester1"
)

type State = int32

const (
	_ State = iota
	follower
	candidate
	leader
)

const (
	votedForNone = -1
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state            State
	lastLeaderCallAt time.Time // last time got leader call

	// Persistent state:

	curTerm  int        // latest term server has seen
	votedFor int        // index of peer in peers
	log      []LogEntry // log entries

	// Volatile state on all servers:

	commitIdx      int // index of highest log entry known to be committed
	lastAppliedIdx int // index of the highest log entry applied to state machine

	// Volatile state leaders only (reinitialized after election):

	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIdx []int
	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIdx []int
}

type LogEntry struct {
	Term int // term when entry was received
	Cmd  any // command for state machine
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.curTerm
	isleader = rf.state == leader
	rf.mu.Unlock()

	return term, isleader
}

func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int // candidate’s term
	CandidateId int // candidate requesting vote
	LastLogIdx  int // index of candidate’s last log entry
	LastLogTerm int // term of candidate’s last log entry
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	VoterId     int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.VoterId = rf.me

	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		return
	}

	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.state = follower
		rf.votedFor = votedForNone
	}

	reply.Term = rf.curTerm

	var lastLogTerm int
	var lastLogIdx int
	if len(rf.log) > 0 {
		lastLogIdx = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIdx].Term
	}
	logOk := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIdx >= lastLogIdx)
	termsOk := args.Term == rf.curTerm
	if termsOk && logOk && (rf.votedFor == votedForNone || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastLeaderCallAt = time.Now()
	}
}

func (rf *Raft) sendRequestVoteRPC(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesRPC(
	server int,
	args *RequestAppendEntriesArgs,
	reply *RequestAppendEntriesReply,
) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type RequestAppendEntriesArgs struct {
	Term            int
	LeaderId        int
	PrevLogIdx      int
	PrevLogTerm     int
	Entries         []LogEntry
	LeaderCommitIdx int
}

type RequestAppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.curTerm

	if args.Term < rf.curTerm {
		return
	}

	rf.state = follower
	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.votedFor = votedForNone
	}

	rf.lastLeaderCallAt = time.Now()

	reply.Success = true
	reply.Term = rf.curTerm
}

func (rf *Raft) startElection() {
	timeout := randElectionIntervalMs()

	var lastLogIdx int
	var lastLogTerm int

	rf.mu.Lock()
	rf.curTerm++
	rf.votedFor = rf.me
	if len(rf.log) > 0 {
		lastLogIdx = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIdx].Term
	}
	currentTerm := rf.curTerm
	rf.lastLeaderCallAt = time.Now()
	rf.mu.Unlock()

	repliesChan := make(chan *RequestVoteReply, len(rf.peers)-1)
	args := &RequestVoteArgs{
		Term:        currentTerm,
		CandidateId: rf.me,
		LastLogIdx:  lastLogIdx,
		LastLogTerm: lastLogTerm,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVoteRPC(idx, args, reply) {
				repliesChan <- reply
			}
		}(i)
	}

	rf.countVotes(timeout, repliesChan)
}

func (rf *Raft) countVotes(timeout time.Duration, repliesChan <-chan *RequestVoteReply) {
	votes := make([]bool, len(rf.peers))
	votes[rf.me] = true

	for {
		select {
		case <-time.After(timeout):
			return
		case reply := <-repliesChan:
			rf.mu.Lock()
			if reply.Term > rf.curTerm {
				rf.curTerm = reply.Term
				rf.state = follower
				rf.votedFor = votedForNone
				rf.mu.Unlock()
				return
			} else if reply.VoteGranted && rf.state == candidate {
				votes[reply.VoterId] = true
				if rf.isEnoughVotes(votes) {
					rf.state = leader
					rf.mu.Unlock()
					rf.sendAppendEntries()
					return
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) isEnoughVotes(votes []bool) bool {
	var vc int
	for _, voted := range votes {
		if voted {
			vc++
		}
	}
	return vc > len(rf.peers)/2
}

func (rf *Raft) sendAppendEntries() {
	timeout := heartbeatIntervalMs()

	rf.mu.Lock()
	curTerm := rf.curTerm
	rf.mu.Unlock()

	repliesChan := make(chan *RequestAppendEntriesReply, len(rf.peers)-1)
	rf.callAppendEntries(repliesChan, curTerm)
	rf.readAppendEntriesReplies(timeout, repliesChan)
}

func (rf *Raft) callAppendEntries(repliesChan chan<- *RequestAppendEntriesReply, term int) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peerIdx int) {
			rf.mu.Lock()
			if rf.curTerm != term || rf.state != leader {
				rf.mu.Unlock()
				return
			}

			args := &RequestAppendEntriesArgs{
				Term:     term,
				LeaderId: rf.me,
				Entries:  make([]LogEntry, 0),
			}
			rf.mu.Unlock()
			reply := &RequestAppendEntriesReply{}
			if rf.sendAppendEntriesRPC(peerIdx, args, reply) {
				repliesChan <- reply
			}
		}(i)
	}
}

func (rf *Raft) readAppendEntriesReplies(timeout time.Duration, repliesChan <-chan *RequestAppendEntriesReply) {
	for {
		select {
		case <-time.After(timeout):
			return
		case reply := <-repliesChan:
			if reply.Success {
				// Should be ok to leave empty for 3A part
			}
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case follower:
			timeout := randElectionIntervalMs()
			time.Sleep(timeout)

			rf.mu.Lock()
			if time.Since(rf.lastLeaderCallAt) >= timeout {
				rf.state = candidate
			}
			rf.mu.Unlock()
		case candidate:
			rf.startElection()
		case leader:
			rf.sendAppendEntries()
		}
	}
}

func randElectionIntervalMs() time.Duration {
	ms := 300 + (rand.Int63() % 300)
	return time.Duration(ms) * time.Millisecond
}
func heartbeatIntervalMs() time.Duration {
	return 50 * time.Millisecond
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = follower
	rf.lastLeaderCallAt = time.Now()
	rf.log = make([]LogEntry, 0)
	rf.nextIdx = make([]int, len(peers))
	rf.matchIdx = make([]int, len(peers))

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
