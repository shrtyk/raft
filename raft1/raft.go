package raft

import (
	"bytes"
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shrtyk/raft/labgob"
	"github.com/shrtyk/raft/labrpc"
	"github.com/shrtyk/raft/raftapi"
	tester "github.com/shrtyk/raft/tester1"
)

type State = uint32

const (
	_ State = iota
	follower
	candidate
	leader
)

const (
	votedForNone = -1
)

const (
	ElectionTimeoutRand = 300 * time.Millisecond
	ElectionTimeoutBase = 300 * time.Millisecond
	HeartbeatInterval   = 70 * time.Millisecond
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state               State
	lastLeaderCallAt    int64 // last time got leader call (unix nano)
	lastAppendEntriesAt int64 // last time leader sent Append Entries (unix nano)

	applyChan  chan raftapi.ApplyMsg // channel for sending back applied messages to fsm
	commitChan chan struct{}         // channel for signaling to applier goroutine

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

	lastIncludedIndex int // the index of the last entry in the log that the snapshot replaces
	lastIncludedTerm  int // the term of the last entry in the log that the snapshot replaces

	// Gracefull shutdown related stuff:

	killCtx    context.Context
	killCancel func()
}

type LogEntry struct {
	Term int // term when entry was received
	Cmd  any // command for state machine
}

// GetState returns current term and whether this server believes it is the leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.curTerm, rf.isState(leader)
}

// getPersistData encodes all non-volatile parameters and returns as slice of bytes
//
// caller must hold lock
func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.curTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	return w.Bytes()
}

// unlockAndPersistIfNeeded unlocks the mutex and persists state if needed.
//
// caller must hold rf.mu.Lock()
func (rf *Raft) unlockAndPersistIfNeeded(shouldPersist bool) {
	if shouldPersist {
		data := rf.getPersistData()
		rf.mu.Unlock()
		rf.persister.Save(data, rf.persister.ReadSnapshot())
	} else {
		rf.mu.Unlock()
	}
}

// readPersist restores previously persisted state
//
// caller must hold lock
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	b := bytes.NewBuffer(data)
	d := labgob.NewDecoder(b)

	var term, lastIncludedTerm, votedFor, lastIncludedIndex int
	var l []LogEntry

	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&l) != nil || d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		log.Fatal("readPersist: decode error")
	}

	rf.curTerm = term
	rf.votedFor = votedFor
	rf.log = l
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm

	rf.commitIdx = rf.lastIncludedIndex
	rf.lastAppliedIdx = rf.lastIncludedIndex
}

func (rf *Raft) PersistBytes() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	// Determine the term of the snapshot point
	term := rf.getTerm(index)

	// Cut the log to keep entries after index
	sliceIndex := index - rf.lastIncludedIndex
	if sliceIndex < len(rf.log) {
		rf.log = append([]LogEntry(nil), rf.log[sliceIndex:]...)
	} else {
		rf.log = nil
	}

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.curTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.Save(data, snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.curTerm
	if args.Term < rf.curTerm {
		rf.mu.Unlock()
		return
	}

	shouldPersist := false
	if args.Term > rf.curTerm {
		shouldPersist = rf.becomeFollower(args.Term)
	}
	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.unlockAndPersistIfNeeded(shouldPersist)
		return
	}

	shouldPersist = true
	sliceIndex := args.LastIncludedIndex - rf.lastIncludedIndex
	if sliceIndex < len(rf.log) && rf.getTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
		rf.log = append([]LogEntry(nil), rf.log[sliceIndex:]...)
	} else {
		rf.log = nil
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if rf.commitIdx < args.LastIncludedIndex {
		rf.commitIdx = args.LastIncludedIndex
	}

	raftStateData := rf.getPersistData()
	snapshotData := args.Data
	rf.mu.Unlock()

	rf.persister.Save(raftStateData, snapshotData)
	rf.signalCommit()
}

func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type RequestVoteArgs struct {
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

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()

	shouldPersist := false
	reply.VoteGranted = false
	reply.VoterId = rf.me

	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		rf.unlockAndPersistIfNeeded(shouldPersist)
		return
	}

	if args.Term > rf.curTerm {
		shouldPersist = rf.becomeFollower(args.Term)
	}

	reply.Term = rf.curTerm
	if rf.isCandidateLogUpToDate(args.LastLogIdx, args.LastLogTerm) &&
		(rf.votedFor == votedForNone || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		shouldPersist = true
		rf.resetElectionTimer()
	}

	rf.unlockAndPersistIfNeeded(shouldPersist)
}

// isCandidateLogUpToDate determines if the candidate's log is at least as up-to-date as receiver's log
//
// caller must hold lock
func (rf *Raft) isCandidateLogUpToDate(candidateLastLogIdx int, candidateLastLogTerm int) bool {
	myLastLogIdx, myLastLogTerm := rf.lastLogIdxAndTerm()
	if candidateLastLogTerm != myLastLogTerm {
		return candidateLastLogTerm > myLastLogTerm
	}
	return candidateLastLogIdx >= myLastLogIdx
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

// Start proposes a new command to be replicated
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	isLeader := rf.isState(leader)
	term := rf.curTerm
	if !isLeader {
		rf.mu.Unlock()
		return -1, term, false
	}

	rf.log = append(rf.log, LogEntry{
		Term: rf.curTerm,
		Cmd:  command,
	})
	data := rf.getPersistData()

	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	rf.matchIdx[rf.me] = lastLogIdx
	rf.nextIdx[rf.me] = lastLogIdx + 1

	rf.mu.Unlock()

	rf.persister.Save(data, rf.persister.ReadSnapshot())

	go rf.sendAppendEntries()

	return lastLogIdx, term, isLeader
}

// Kill sets the peer to a dead state
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.killCancel()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

type RequestAppendEntriesArgs struct {
	Term            int        // leader term
	LeaderId        int        // for riderection
	PrevLogTerm     int        // term of prevLogIdx entry
	PrevLogIdx      int        // index of log entry immidiately preceding new ones
	LeaderCommitIdx int        // leader's commit index
	Entries         []LogEntry // log entries to store (empty for heartbeat)
}

type RequestAppendEntriesReply struct {
	Term    int  // current term for leader to update itself
	Success bool // true if follower contained entry matching prevLogIdx and prevLogTerm

	ConflictIdx  int
	ConflictTerm int
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()

	shouldPersist := false
	reply.Success = false
	reply.Term = rf.curTerm

	if args.Term < rf.curTerm {
		rf.unlockAndPersistIfNeeded(shouldPersist)
		return
	}

	if args.Term > rf.curTerm {
		shouldPersist = rf.becomeFollower(args.Term)
	}

	rf.resetElectionTimer()
	reply.Term = rf.curTerm

	if args.PrevLogIdx < rf.lastIncludedIndex {
		reply.Success = false
		rf.unlockAndPersistIfNeeded(shouldPersist)
		return
	}

	lastLogAbsIdx, _ := rf.lastLogIdxAndTerm()
	if args.PrevLogIdx > lastLogAbsIdx || (args.PrevLogIdx >= rf.lastIncludedIndex && rf.getTerm(args.PrevLogIdx) != args.PrevLogTerm) {
		rf.fillConflictReply(args, reply)
		rf.unlockAndPersistIfNeeded(shouldPersist)
		return
	}

	shouldPersist = shouldPersist || rf.processEntries(args)
	if args.LeaderCommitIdx > rf.commitIdx {
		lastLogIndex, _ := rf.lastLogIdxAndTerm()
		rf.commitIdx = min(args.LeaderCommitIdx, lastLogIndex)
		rf.signalCommit()
	}

	reply.Success = true
	rf.unlockAndPersistIfNeeded(shouldPersist)
}

// processEntries handles appending/truncating entries to the follower's log.
// It returns true if the log was modified.
//
// caller must hold lock
func (rf *Raft) processEntries(args *RequestAppendEntriesArgs) (logChanged bool) {
	for i, entry := range args.Entries {
		absIdx := args.PrevLogIdx + 1 + i
		lastAbsIdx, _ := rf.lastLogIdxAndTerm()
		if absIdx > lastAbsIdx {
			rf.log = append(rf.log, args.Entries[i:]...)
			logChanged = true
			break
		}

		if rf.getTerm(absIdx) != entry.Term {
			sliceIdx := absIdx - rf.lastIncludedIndex - 1
			rf.log = rf.log[:sliceIdx]
			rf.log = append(rf.log, args.Entries[i:]...)
			logChanged = true
			break
		}
	}
	return
}

// fillConflictReply sets the conflict fields in an AppendEntries reply
//
// caller must hold lock
func (rf *Raft) fillConflictReply(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	if args.PrevLogIdx > lastLogIdx {
		reply.ConflictIdx = lastLogIdx + 1
		reply.ConflictTerm = -1
	} else {
		reply.ConflictTerm = rf.getTerm(args.PrevLogIdx)
		firstIndexOfTerm := args.PrevLogIdx
		for firstIndexOfTerm > rf.lastIncludedIndex+1 && rf.getTerm(firstIndexOfTerm-1) == reply.ConflictTerm {
			firstIndexOfTerm--
		}
		reply.ConflictIdx = firstIndexOfTerm
	}
}

// startElection begins a new election
func (rf *Raft) startElection() {
	timeout := randElectionIntervalMs()

	rf.mu.Lock()
	rf.curTerm++
	rf.votedFor = rf.me
	data := rf.getPersistData()
	rf.resetElectionTimer()
	lastLogIdx, lastLogTerm := rf.lastLogIdxAndTerm()
	currentTerm := rf.curTerm
	rf.mu.Unlock()

	rf.persister.Save(data, rf.persister.ReadSnapshot())

	repliesChan := make(chan *RequestVoteReply, len(rf.peers)-1)
	args := &RequestVoteArgs{
		Term:        currentTerm,
		CandidateId: rf.me,
		LastLogIdx:  lastLogIdx,
		LastLogTerm: lastLogTerm,
	}
	for i := range rf.peers {
		if i == int(rf.me) {
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
				shouldPersist := rf.becomeFollower(reply.Term)
				rf.resetElectionTimer()
				rf.unlockAndPersistIfNeeded(shouldPersist)
				return
			} else if reply.VoteGranted && rf.isState(candidate) {
				votes[reply.VoterId] = true
				if rf.isEnoughVotes(votes) {
					rf.becomeLeader()
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
	rf.mu.RLock()
	curTerm := rf.curTerm
	rf.mu.RUnlock()

	rf.resetHeartbeatTimer()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peerIdx int) {
			rf.mu.RLock()
			if rf.curTerm != curTerm || !rf.isState(leader) {
				rf.mu.RUnlock()
				return
			}

			if rf.nextIdx[peerIdx] <= rf.lastIncludedIndex {
				rf.leaderSendSnapshot(peerIdx)
			} else {
				rf.leaderSendEntries(peerIdx)
			}
		}(i)
	}
}

// leaderSendSnapshot handles sending a snapshot to a single peer.
//
// Assumes the lock is held when called
func (rf *Raft) leaderSendSnapshot(peerIdx int) {
	args := &InstallSnapshotArgs{
		Term:              rf.curTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.RUnlock()

	reply := &InstallSnapshotReply{}
	if rf.sendInstallSnapshotRPC(peerIdx, args, reply) {
		rf.mu.Lock()

		if rf.curTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		if reply.Term > rf.curTerm {
			shouldPersist := rf.becomeFollower(reply.Term)
			rf.resetElectionTimer()
			rf.unlockAndPersistIfNeeded(shouldPersist)
			return
		}

		rf.matchIdx[peerIdx] = max(rf.matchIdx[peerIdx], args.LastIncludedIndex)
		rf.nextIdx[peerIdx] = rf.matchIdx[peerIdx] + 1
		rf.mu.Unlock()
	}
}

// leaderSendEntries handles sending log entries to a single peer.
//
// Assumes the lock is held when called
func (rf *Raft) leaderSendEntries(peerIdx int) {
	prevLogIdx := rf.nextIdx[peerIdx] - 1
	prevLogTerm := rf.getTerm(prevLogIdx)

	sliceIndex := rf.nextIdx[peerIdx] - rf.lastIncludedIndex - 1
	entries := make([]LogEntry, len(rf.log[sliceIndex:]))
	copy(entries, rf.log[sliceIndex:])

	args := &RequestAppendEntriesArgs{
		Term:            rf.curTerm,
		LeaderId:        rf.me,
		PrevLogIdx:      prevLogIdx,
		PrevLogTerm:     prevLogTerm,
		LeaderCommitIdx: rf.commitIdx,
		Entries:         entries,
	}
	rf.mu.RUnlock()

	reply := &RequestAppendEntriesReply{}
	if rf.sendAppendEntriesRPC(peerIdx, args, reply) {
		rf.mu.Lock()

		if rf.curTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		shouldPersist := rf.handleAppendEntriesReply(peerIdx, args, reply)
		rf.unlockAndPersistIfNeeded(shouldPersist)
	}
}

// handleAppendEntriesReply processes the reply from an AppendEntries RPC
//
// caller must hold lock
func (rf *Raft) handleAppendEntriesReply(peerIdx int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) (shouldPersist bool) {
	if reply.Term > rf.curTerm {
		shouldPersist = rf.becomeFollower(reply.Term)
		rf.resetElectionTimer()
		return
	}

	if !rf.isState(leader) || args.Term != rf.curTerm {
		return
	}

	if reply.Success {
		newMatchIdx := args.PrevLogIdx + len(args.Entries)
		if newMatchIdx > rf.matchIdx[peerIdx] {
			rf.matchIdx[peerIdx] = newMatchIdx
		}
		rf.nextIdx[peerIdx] = rf.matchIdx[peerIdx] + 1

		lastCommitIdx := rf.commitIdx
		rf.tryToCommit()
		if rf.commitIdx != lastCommitIdx {
			rf.signalCommit()
		}
		return
	}

	if reply.ConflictTerm >= 0 {
		lastIdxTerm := -1
		lastLogIdx, _ := rf.lastLogIdxAndTerm()
		for i := lastLogIdx; i > rf.lastIncludedIndex; i-- {
			if rf.getTerm(i) == reply.ConflictTerm {
				lastIdxTerm = i
				break
			}
		}

		if lastIdxTerm >= 0 {
			rf.nextIdx[peerIdx] = lastIdxTerm + 1
		} else {
			rf.nextIdx[peerIdx] = reply.ConflictIdx
		}
	} else {
		rf.nextIdx[peerIdx] = reply.ConflictIdx
	}
	return
}

func (rf *Raft) tryToCommit() {
	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	for i := rf.commitIdx + 1; i <= lastLogIdx; i++ {
		if rf.getTerm(i) != rf.curTerm {
			continue
		}

		count := 1
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			if rf.matchIdx[peer] >= i {
				count++
			}
		}

		if count > len(rf.peers)/2 && i > rf.commitIdx {
			rf.commitIdx = i
		}
	}
}

func (rf *Raft) hasTimedOut(lastTimestamp, timeout int64) bool {
	return time.Now().UnixNano()-lastTimestamp >= timeout
}

// ticker is the main state machine loop for a Raft peer
func (rf *Raft) ticker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			switch atomic.LoadUint32(&rf.state) {
			case follower:
				timeout := randElectionIntervalMs()
				time.Sleep(timeout)

				// Important note:
				// We forced to use lock here since conditions check + role transition should be an atomic operation
				rf.mu.Lock()
				lastCall := atomic.LoadInt64(&rf.lastLeaderCallAt)
				if !rf.killed() && rf.isState(follower) && rf.hasTimedOut(lastCall, timeout.Nanoseconds()) {
					atomic.StoreUint32(&rf.state, candidate)
				}
				rf.mu.Unlock()
			case candidate:
				rf.startElection()
			case leader:
				time.Sleep(HeartbeatInterval)
				lastBeat := atomic.LoadInt64(&rf.lastAppendEntriesAt)
				if !rf.killed() && rf.hasTimedOut(lastBeat, HeartbeatInterval.Nanoseconds()) {
					rf.sendAppendEntries()
				}
			}
		}
	}
}

// sendAppliedMessage helper function to send msg into applyChan
func (rf *Raft) sendAppliedMessage(ctx context.Context, msg *raftapi.ApplyMsg) {
	select {
	case <-ctx.Done():
		return
	case rf.applyChan <- *msg:
	}
}

// applies committed log entries to the state machine in the background
func (rf *Raft) applier(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-rf.commitChan:
			rf.mu.RLock()
			// Prioritize applying a snapshot if one is pending
			if rf.lastAppliedIdx < rf.lastIncludedIndex {
				msg := raftapi.ApplyMsg{
					SnapshotValid: true,
					Snapshot:      rf.persister.ReadSnapshot(),
					SnapshotTerm:  rf.lastIncludedTerm,
					SnapshotIndex: rf.lastIncludedIndex,
				}
				rf.lastAppliedIdx = max(rf.lastAppliedIdx, msg.SnapshotIndex)
				rf.mu.RUnlock()

				rf.sendAppliedMessage(ctx, &msg)
				continue
			}

			// Batch apply committed log entries
			if rf.lastAppliedIdx < rf.commitIdx {
				start := rf.lastAppliedIdx + 1
				end := rf.commitIdx

				msgs := make([]raftapi.ApplyMsg, 0, end-start+1)
				for i := start; i <= end; i++ {
					sliceIdx := i - rf.lastIncludedIndex - 1
					msgs = append(msgs, raftapi.ApplyMsg{
						CommandValid: true,
						Command:      rf.log[sliceIdx].Cmd,
						CommandIndex: i,
					})
				}
				rf.lastAppliedIdx = end
				rf.mu.RUnlock()

				for _, msg := range msgs {
					rf.sendAppliedMessage(ctx, &msg)
				}
			} else {
				rf.mu.RUnlock()
			}
		}
	}
}

// getTerm returns the term of a log entry at a given absolute index.
// It handles cases where the index is part of a snapshot.
//
// caller must hold lock
func (rf *Raft) getTerm(idx int) int {
	if idx == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}

	if idx < rf.lastIncludedIndex {
		return -1
	}

	sliceIndex := idx - rf.lastIncludedIndex - 1
	if sliceIndex >= len(rf.log) {
		return -1
	}
	return rf.log[sliceIndex].Term
}

// lastLogIdxAndTerm returns the index and term of the last entry in the log
//
// caller must hold lock
func (rf *Raft) lastLogIdxAndTerm() (lastLogIdx, lastLogTerm int) {
	if len(rf.log) > 0 {
		lastLogIdx = rf.lastIncludedIndex + len(rf.log)
		lastLogTerm = rf.log[len(rf.log)-1].Term
	} else {
		lastLogIdx = rf.lastIncludedIndex
		lastLogTerm = rf.lastIncludedTerm
	}
	return
}

func (rf *Raft) isState(state State) bool {
	return atomic.LoadUint32(&rf.state) == state
}

// becomeFollower transitions the peer to the follower state.
// It returns true if state change requiring persistence occured.
//
// caller must hold lock
func (rf *Raft) becomeFollower(term int) (stateChanged bool) {
	atomic.StoreUint32(&rf.state, follower)
	if term > rf.curTerm {
		rf.curTerm = term
		rf.votedFor = votedForNone
		stateChanged = true
	}
	return
}

// becomeLeader transitions the peer to the leader state
//
// caller must hold lock
func (rf *Raft) becomeLeader() {
	atomic.StoreUint32(&rf.state, leader)
	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	for i := range rf.peers {
		rf.nextIdx[i] = lastLogIdx + 1
		rf.matchIdx[i] = 0
	}
	rf.matchIdx[rf.me] = lastLogIdx
}

// resetElectionTimer resets the election timer
func (rf *Raft) resetElectionTimer() {
	atomic.StoreInt64(&rf.lastLeaderCallAt, time.Now().UnixNano())
}

// resetHeartbeatTimer resets the heartbeat timer
func (rf *Raft) resetHeartbeatTimer() {
	atomic.StoreInt64(&rf.lastAppendEntriesAt, time.Now().UnixNano())
}

// signalCommit sends signal to applier goroutine
func (rf *Raft) signalCommit() {
	select {
	case rf.commitChan <- struct{}{}:
	default:
	}
}

func randElectionIntervalMs() time.Duration {
	return ElectionTimeoutBase + time.Duration(rand.Int63n(int64(ElectionTimeoutRand)))
}

// Make creates and starts a new Raft peer
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	ctx, cancel := context.WithCancel(context.Background())
	rf.killCtx = ctx
	rf.killCancel = cancel
	rf.commitChan = make(chan struct{}, 1)

	atomic.StoreUint32(&rf.state, follower)
	rf.log = make([]LogEntry, 0)
	rf.applyChan = applyCh

	rf.readPersist(persister.ReadRaftState())

	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	rf.nextIdx = make([]int, len(peers))
	for i := range rf.nextIdx {
		rf.nextIdx[i] = lastLogIdx + 1
	}
	rf.matchIdx = make([]int, len(peers))

	rf.resetElectionTimer()
	go rf.applier(ctx)
	go rf.ticker(ctx)

	return rf
}
