package raft

import (
	"bytes"
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

const (
	ElectionTimeoutRand = 300 * time.Millisecond
	ElectionTimeoutBase = 300 * time.Millisecond
	HeartbeatInterval   = 70 * time.Millisecond
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

	state               State
	lastLeaderCallAt    time.Time // last time got leader call
	lastAppendEntriesAt time.Time // last time leader sent Append Entries

	applyChan  chan raftapi.ApplyMsg
	commitCond *sync.Cond

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

	lastIncludedIndex int
	lastIncludedTerm  int
}

type LogEntry struct {
	Term int // term when entry was received
	Cmd  any // command for state machine
}

// GetState returns current term and whether this server believes it is the leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.curTerm, rf.state == leader
}

// persist saves Raft's persistent state to stable storage
//
// caller must hold lock
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.curTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	data := w.Bytes()
	rf.persister.Save(data, nil)
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

	var term, votedFor, lastIncludedIndex, lastIncludedTerm int
	var l []LogEntry

	if err := d.Decode(&term); err != nil {
		log.Println(err)
		return
	}

	if err := d.Decode(&votedFor); err != nil {
		log.Println(err)
		return
	}

	if err := d.Decode(&l); err != nil {
		log.Println(err)
		return
	}

	if err := d.Decode(&lastIncludedIndex); err != nil {
		log.Println(err)
		return
	}

	if err := d.Decode(&lastIncludedTerm); err != nil {
		log.Println(err)
		return
	}

	rf.curTerm = term
	rf.votedFor = votedFor
	rf.log = l
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// TODO

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
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.VoterId = rf.me

	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		return
	}

	if args.Term > rf.curTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.curTerm
	if rf.isCandidateLogUpToDate(args.LastLogIdx, args.LastLogTerm) &&
		(rf.votedFor == votedForNone || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
	}
}

// isCandidateLogUpToDate determines if the candidate's log is at least as up-to-date as receiver's log
//
// caller must hold lock
func (rf *Raft) isCandidateLogUpToDate(candidateLastLogIdx, candidateLastLogTerm int) bool {
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
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()

	isLeader = rf.state == leader
	term = rf.curTerm
	if !isLeader {
		rf.mu.Unlock()
		// redirect to leader node
		return index, term, isLeader
	}

	DPrintf("S%d T%d L: received new command", rf.me, rf.curTerm)
	rf.log = append(rf.log, LogEntry{
		Term: rf.curTerm,
		Cmd:  command,
	})
	rf.persist()
	index = len(rf.log)
	rf.matchIdx[rf.me] = index - 1
	rf.mu.Unlock()
	rf.sendAppendEntries()

	return index, term, isLeader
}

// Kill sets the peer to a dead state
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.curTerm

	if args.Term < rf.curTerm {
		return
	}

	if args.Term > rf.curTerm {
		rf.becomeFollower(args.Term)
	}

	rf.resetElectionTimer()
	reply.Term = rf.curTerm
	if args.PrevLogIdx >= 0 && (args.PrevLogIdx >= len(rf.log) || rf.log[args.PrevLogIdx].Term != args.PrevLogTerm) {
		rf.fillConflictReply(args, reply)
		return
	}

	rf.processEntries(args)
	if args.LeaderCommitIdx > rf.commitIdx {
		lastLogIndex := len(rf.log) - 1
		rf.commitIdx = min(args.LeaderCommitIdx, lastLogIndex)
		rf.commitCond.Broadcast()
	}

	reply.Success = true
}

// processEntries handles appending/truncating entries to the follower's log
//
// caller must hold lock
func (rf *Raft) processEntries(args *RequestAppendEntriesArgs) {
	logInsertIdx := args.PrevLogIdx + 1
	newEntriesIdx := 0

	for {
		if logInsertIdx >= len(rf.log) || newEntriesIdx >= len(args.Entries) {
			break
		}
		if rf.log[logInsertIdx].Term != args.Entries[newEntriesIdx].Term {
			break
		}
		logInsertIdx++
		newEntriesIdx++
	}

	if newEntriesIdx < len(args.Entries) {
		rf.log = append(rf.log[:logInsertIdx], args.Entries[newEntriesIdx:]...)
		rf.persist()
	}
}

// fillConflictReply sets the conflict fields in an AppendEntries reply
//
// caller must hold lock
func (rf *Raft) fillConflictReply(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	if args.PrevLogIdx >= len(rf.log) {
		reply.ConflictIdx = len(rf.log)
		reply.ConflictTerm = -1
	} else {
		reply.ConflictTerm = rf.log[args.PrevLogIdx].Term

		firstIndexOfTerm := args.PrevLogIdx
		for firstIndexOfTerm > 0 && rf.log[firstIndexOfTerm-1].Term == reply.ConflictTerm {
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
	rf.persist()
	rf.resetElectionTimer()
	lastLogIdx, lastLogTerm := rf.lastLogIdxAndTerm()
	currentTerm := rf.curTerm
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
				rf.becomeFollower(reply.Term)
				rf.resetElectionTimer()
				rf.mu.Unlock()
				return
			} else if reply.VoteGranted && rf.state == candidate {
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
	rf.mu.Lock()
	rf.resetHeartbeatTimer()
	curTerm := rf.curTerm
	rf.mu.Unlock()

	rf.callAppendEntries(curTerm)
}

func (rf *Raft) callAppendEntries(term int) {
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

			prevLogIdx := rf.nextIdx[peerIdx] - 1
			prevLogTerm := -1
			if prevLogIdx >= 0 {
				if prevLogIdx >= len(rf.log) {
					rf.mu.Unlock()
					return
				}
				prevLogTerm = rf.log[prevLogIdx].Term
			}
			entries := rf.log[rf.nextIdx[peerIdx]:]
			entriesCopy := make([]LogEntry, len(entries))
			copy(entriesCopy, entries)

			args := &RequestAppendEntriesArgs{
				Term:            term,
				LeaderId:        rf.me,
				PrevLogIdx:      prevLogIdx,
				PrevLogTerm:     prevLogTerm,
				LeaderCommitIdx: rf.commitIdx,
				Entries:         entriesCopy,
			}
			rf.mu.Unlock()

			reply := &RequestAppendEntriesReply{}
			if rf.sendAppendEntriesRPC(peerIdx, args, reply) {
				rf.mu.Lock()
				if rf.curTerm != term || rf.state != leader {
					rf.mu.Unlock()
					return
				}

				rf.handleAppendEntriesReply(peerIdx, args, reply)

				if reply.Success {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			} else {
				return
			}
		}(i)
	}
}

// handleAppendEntriesReply processes the reply from an AppendEntries RPC
//
// caller must hold lock
func (rf *Raft) handleAppendEntriesReply(peerIdx int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	if reply.Term > rf.curTerm {
		rf.becomeFollower(reply.Term)
		rf.resetElectionTimer()
		return
	}

	if rf.state != leader || args.Term != rf.curTerm {
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
			rf.commitCond.Broadcast()
		}
		return
	}

	if reply.ConflictTerm >= 0 {
		lastIdxTerm := -1
		for i := len(rf.log) - 1; i >= 0; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
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
}

// tryToCommit advances the commit index if possible
//
// caller must hold lock
func (rf *Raft) tryToCommit() {
	for i := rf.commitIdx + 1; i < len(rf.log); i++ {
		if rf.log[i].Term != rf.curTerm {
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

// ticker is the main state machine loop for a Raft peer
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
			if rf.state == follower && time.Since(rf.lastLeaderCallAt) >= timeout {
				rf.state = candidate
				DPrintf("S%d T%d F: election timeout", rf.me, rf.curTerm)
			}
			rf.mu.Unlock()
		case candidate:
			rf.startElection()
		case leader:
			time.Sleep(HeartbeatInterval)
			rf.mu.Lock()
			if time.Since(rf.lastAppendEntriesAt) >= HeartbeatInterval {
				rf.mu.Unlock()
				rf.sendAppendEntries()
			} else {
				rf.mu.Unlock()
			}
		}
	}
}

// applier applies committed log entries to the state machine in the background
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIdx <= rf.lastAppliedIdx && !rf.killed() {
			rf.commitCond.Wait()
		}

		lastApplied := rf.lastAppliedIdx
		commitIdx := rf.commitIdx
		if commitIdx <= lastApplied {
			rf.mu.Unlock()
			continue
		}

		msgs := make([]raftapi.ApplyMsg, 0, commitIdx-lastApplied)
		for i := lastApplied + 1; i <= commitIdx; i++ {
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Cmd,
				CommandIndex: i + 1,
			})
		}
		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.applyChan <- msg
		}
		rf.lastAppliedIdx = commitIdx
	}
}

// lastLogIdxAndTerm returns the index and term of the last entry in the log
//
// caller must hold lock
func (rf *Raft) lastLogIdxAndTerm() (lastLogIdx int, lastLogTerm int) {
	lastLogIdx, lastLogTerm = -1, -1
	if len(rf.log) > 0 {
		lastLogIdx = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIdx].Term
	}
	return
}

// becomeFollower transitions the peer to the follower state
//
// caller must hold lock
func (rf *Raft) becomeFollower(term int) {
	rf.state = follower
	if term > rf.curTerm {
		rf.curTerm = term
		rf.votedFor = votedForNone
		rf.persist()
	}
}

// becomeLeader transitions the peer to the leader state
//
// caller must hold lock
func (rf *Raft) becomeLeader() {
	rf.state = leader
	lastLogIdx, _ := rf.lastLogIdxAndTerm()
	for i := range rf.peers {
		rf.nextIdx[i] = lastLogIdx + 1
		rf.matchIdx[i] = -1
	}
	rf.matchIdx[rf.me] = lastLogIdx
}

// resetElectionTimer resets the election timer
//
// caller must hold lock
func (rf *Raft) resetElectionTimer() {
	rf.lastLeaderCallAt = time.Now()
}

// resetHeartbeatTimer resets the heartbeat timer
//
// caller must hold lock
func (rf *Raft) resetHeartbeatTimer() {
	rf.lastAppendEntriesAt = time.Now()
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

	rf.commitCond = sync.NewCond(&rf.mu)

	rf.state = follower
	rf.lastLeaderCallAt = time.Now()
	rf.log = make([]LogEntry, 0)
	rf.commitIdx = -1
	rf.lastAppliedIdx = -1
	rf.applyChan = applyCh
	rf.nextIdx = make([]int, len(peers))
	rf.matchIdx = make([]int, len(peers))
	for i := range rf.matchIdx {
		rf.matchIdx[i] = -1
	}

	rf.readPersist(persister.ReadRaftState())

	go rf.applier()
	go rf.ticker()

	return rf
}
