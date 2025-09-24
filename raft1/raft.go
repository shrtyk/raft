package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shrtyk/raft/labrpc"
	"github.com/shrtyk/raft/raftapi"
	tester "github.com/shrtyk/raft/tester1"
)

const (
	RPCTimeout = 500 * time.Millisecond
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

	curTerm  int64      // latest term server has seen
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

	applyChan chan raftapi.ApplyMsg
}

type LogEntry struct {
	Term int64 // term when entry was received
	Cmd  any   // command for state machine
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	term = int(rf.curTerm) // potential bug if applicaion will be booted on 32bit systems
	isleader = rf.state == leader
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int64 // candidate’s term
	CandidateId int   // candidate requesting vote
	LastLogIdx  int   // index of candidate’s last log entry
	LastLogTerm int64 // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int64
	VoteGranted bool
	VoterId     int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

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
	var lastLogTerm int64 = -1
	var lastLogIdx int = -1
	if len(rf.log) > 0 {
		lastLogIdx = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIdx].Term
	}

	logIsUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIdx >= lastLogIdx)
	if args.Term == rf.curTerm && logIsUpToDate && (rf.votedFor == votedForNone || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		reply.VoterId = rf.me
		rf.votedFor = args.CandidateId
		rf.lastLeaderCallAt = time.Now()
	}
}

type RequestAppendEntriesArgs struct {
	Term            int64      // leader term
	LeaderId        int        // for riderection
	PrevLogTerm     int64      // term of prevLogIdx entry
	PrevLogIdx      int        // index of log entry immidiately preceding new ones
	LeaderCommitIdx int        // leader's commit index
	Entries         []LogEntry // log entries to store (empty for heartbeat)
}

type RequestAppendEntriesReply struct {
	Term    int64 // current term for leader to update itself
	Success bool  // true if follower contained entry matching prevLogIdx and prevLogTerm
}

func (rf *Raft) AppendEntriesRPC(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		return
	}

	rf.state = follower
	rf.lastLeaderCallAt = time.Now()
	if args.Term > rf.curTerm {
		rf.curTerm = args.Term
		rf.votedFor = votedForNone
	}

	reply.Term = rf.curTerm

	if args.PrevLogIdx >= 0 &&
		(args.PrevLogIdx >= len(rf.log) || rf.log[args.PrevLogIdx].Term != args.PrevLogTerm) {
		return
	}

	rf.appendEntries(args)
	if args.LeaderCommitIdx > rf.commitIdx {
		idxOfLastNewEntry := args.PrevLogIdx + len(args.Entries)
		rf.commitIdx = min(args.LeaderCommitIdx, idxOfLastNewEntry)
		// Apply entries from lastAppliedIdx up to commitIdx
	}
	reply.Success = true

	rf.lastLeaderCallAt = time.Now()
}

func (rf *Raft) appendEntries(args *RequestAppendEntriesArgs) {
	// This function assumes the PrevLogIdx/Term check has already passed in the caller.

	// Find the index of the first entry to append. This is also where we'll truncate.
	insertionIdx := 0
	for ; insertionIdx < len(args.Entries); insertionIdx++ {
		logIdx := args.PrevLogIdx + 1 + insertionIdx
		if logIdx >= len(rf.log) || rf.log[logIdx].Term != args.Entries[insertionIdx].Term {
			break
		}
	}

	// Truncate the log at the first point of conflict/divergence.
	logIdxToTruncateAt := args.PrevLogIdx + 1 + insertionIdx
	if logIdxToTruncateAt <= len(rf.log) {
		rf.log = rf.log[:logIdxToTruncateAt]
	}

	// Append the new entries from the leader.
	if insertionIdx < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[insertionIdx:]...)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesRPC(
	server int,
	args *RequestAppendEntriesArgs,
	reply *RequestAppendEntriesReply,
) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()

	isLeader = rf.state == leader
	if !isLeader {
		rf.mu.Unlock()
		// TODO: redirect to leader node
		return index, term, isLeader
	}

	rf.log = append(rf.log, LogEntry{
		Term: rf.curTerm,
		Cmd:  command,
	})
	index = len(rf.log)
	rf.matchIdx[rf.me] = index - 1
	term = int(rf.curTerm)
	rf.mu.Unlock()

	rf.replicateLog()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	var lastLogIdx int = -1
	var lastLogTerm int64 = -1

	rf.mu.Lock()
	rf.curTerm++
	rf.votedFor = rf.me
	if len(rf.log) > 0 {
		lastLogIdx = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIdx].Term
	}
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
			if rf.sendRequestVote(idx, args, reply) {
				repliesChan <- reply
			}
		}(i)
	}
	go rf.countVotes(repliesChan)
}

func (rf *Raft) isEnoughVotes(votes []bool) bool {
	var v int
	for _, voted := range votes {
		if voted {
			v++
		}
	}
	return v >= len(rf.peers)/2+1
}

func (rf *Raft) countVotes(repliesChan chan *RequestVoteReply) {
	votes := make([]bool, len(rf.peers))
	votes[rf.me] = true

	for {
		select {
		case <-time.After(RPCTimeout):
			return
		case reply := <-repliesChan:
			rf.mu.Lock()
			if reply.Term > rf.curTerm {
				// step down and become follower
				rf.curTerm = reply.Term
				rf.state = follower
				rf.votedFor = votedForNone
				rf.lastLeaderCallAt = time.Now()
				rf.mu.Unlock()
				return
			} else if reply.VoteGranted && rf.state == candidate {
				votes[reply.VoterId] = true
				if rf.isEnoughVotes(votes) {
					rf.state = leader
					for i := range rf.peers {
						rf.nextIdx[i] = len(rf.log)
						if i == rf.me {
							rf.matchIdx[i] = len(rf.log) - 1
						} else {
							rf.matchIdx[i] = -1
						}
					}
					rf.mu.Unlock()
					go rf.replicateLog()
					return
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) replicateLog() {
	rf.mu.Lock()
	curTerm := rf.curTerm
	commitIdx := rf.commitIdx
	rf.lastLeaderCallAt = time.Now()
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			rf.mu.Lock()
			if rf.state != leader {
				rf.mu.Unlock()
				return
			}
			nextIdx := rf.nextIdx[idx]
			prevLogIdx := nextIdx - 1
			var prevLogTerm int64 = -1
			if prevLogIdx >= 0 {
				if prevLogIdx >= len(rf.log) {
					rf.nextIdx[idx] = len(rf.log)
					rf.mu.Unlock()
					return
				}
				prevLogTerm = rf.log[prevLogIdx].Term
			}

			// Make a deep copy of the entries to send. This is crucial to avoid a data race,
			// as the RPC system will read the entries after the lock is released.
			entries := make([]LogEntry, len(rf.log[nextIdx:]))
			copy(entries, rf.log[nextIdx:])

			args := &RequestAppendEntriesArgs{
				Term:            curTerm,
				LeaderId:        rf.me,
				PrevLogTerm:     prevLogTerm,
				PrevLogIdx:      prevLogIdx,
				LeaderCommitIdx: commitIdx,
				Entries:         entries,
			}
			rf.mu.Unlock()

			reply := &RequestAppendEntriesReply{}
			if !rf.sendAppendEntriesRPC(idx, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if args.Term != rf.curTerm || rf.state != leader {
				return
			}

			if reply.Success {
				rf.matchIdx[idx] = prevLogIdx + len(entries)
				rf.nextIdx[idx] = rf.matchIdx[idx] + 1
				rf.tryToCommit()
			} else {
				if reply.Term > rf.curTerm {
					rf.curTerm = reply.Term
					rf.state = follower
					rf.votedFor = votedForNone
					rf.lastLeaderCallAt = time.Now()
				} else if rf.nextIdx[idx] > 0 {
					rf.nextIdx[idx]--
				}
			}
		}(i)
	}
}

func (rf *Raft) tryToCommit() {
	for i := rf.commitIdx + 1; i < len(rf.log); i++ {
		var count int
		for _, idx := range rf.matchIdx {
			if idx >= i {
				count++
			}
		}

		if count >= len(rf.peers)/2+1 && rf.log[i].Term == rf.curTerm {
			// TODO: commit
			rf.commitIdx = i
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
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
			go rf.startElection()

			timeout := randElectionIntervalMs()
			time.Sleep(timeout)
		case leader:
			rf.replicateLog()

			timeout := heartbeatIntervalMs()
			time.Sleep(timeout)
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIdx > rf.lastAppliedIdx {
			msgs := make([]raftapi.ApplyMsg, 0, rf.commitIdx-rf.lastAppliedIdx)

			for i := rf.lastAppliedIdx + 1; i <= rf.commitIdx; i++ {
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

			rf.mu.Lock()
			rf.lastAppliedIdx += len(msgs)
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
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

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = follower
	rf.votedFor = votedForNone
	rf.lastLeaderCallAt = time.Now()
	rf.log = make([]LogEntry, 0)
	rf.nextIdx = make([]int, len(peers))
	rf.matchIdx = make([]int, len(peers))
	rf.applyChan = applyCh
	rf.commitIdx = -1
	rf.lastAppliedIdx = -1

	for i := range rf.matchIdx {
		rf.matchIdx[i] = -1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.applier()

	go rf.ticker()

	return rf
}
