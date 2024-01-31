package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	electionTimeoutLowerBound      = 750
	electionTimeoutVariablePortion = 750
	// Tests require <10 heartbeats per second.
	heartbeatInterval = 200
)

const (
	Follower = iota
	Candidate
	Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                *sync.Mutex         // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	hasEntriesToApply *sync.Cond

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state on all servers
	currentTerm int        // Latest term server has seen (initialized as 0 and increases monotonically)
	votedFor    int        // CandidateId that received vote in current term (-1 if none)
	log         []LogEntry // Log entries

	status                   int
	effectiveFirstIndex      int
	snapshotLastIncludedTerm int

	// Volatile state on all servers
	commitIndex int // Highest index of committed log entry (initialized as 0 and increases monotonically)
	lastApplied int // Highest index of log entry applied to SM (initialized as 0 and increases monotonically)

	snapshot    []byte
	msgsToApply []ApplyMsg

	// Volatile state on leaders
	nextIndex  []int // For each server, index of next entry to send to that server (initialized as leader's last log index + 1)
	matchIndex []int // For each server, highest index of replicated entries (initialized as 0 and increases monotonically)

	electionTimeoutStartTime   time.Time // Last time server received AppendEntries from current leader or granted vote to candidate
	heartbeatIntervalStartTime time.Time // Last time the leader sent an AppendEntries call to followers
	applyCh                    chan ApplyMsg
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	// Your initialization code here (2A, 2B, 2C).
	// fmt.Printf("Server %v booting\n", me)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0}) // Appends the first log entry for initial RPCs to have valid fields
	rf.nextIndex = nil
	rf.matchIndex = nil
	var mu sync.Mutex
	rf.mu = &mu
	rf.hasEntriesToApply = sync.NewCond(rf.mu)
	rf.electionTimeoutStartTime = time.Now() // Starts a fresh election timeout clock
	rf.status = Follower
	rf.applyCh = applyCh
	rf.effectiveFirstIndex = 0
	rf.snapshot = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	if rf.status == Leader {
		rf.nextIndex = make([]int, len(rf.peers))
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log) + rf.effectiveFirstIndex
		}
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
			// if rf.effectiveFirstIndex > 1 {
			// 	rf.matchIndex[i] = rf.effectiveFirstIndex - 1
			// }
		}
	}

	// start ticker goroutine to start elections
	go rf.electionTicker()

	// start ticker goroutine to apply committed entries
	go rf.applyTicker()

	return rf
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.status == Leader
	return term, isleader
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
	// Your code here (2B).
	rf.mu.Lock()
	// // fmt.Printf("Server %v (status = %v) called by Start()\n", rf.me, rf.status)
	defer rf.mu.Unlock()
	defer rf.persist()

	var index int
	term := rf.currentTerm
	isLeader := rf.status == Leader && rf.killed() == false

	if isLeader {
		// // fmt.Printf("Server %v (status = %v) calling startAgreement()\n", rf.me, rf.status)
		newLogEntry := LogEntry{Command: command, Term: rf.currentTerm}
		rf.log = append(rf.log, newLogEntry)
		rf.nextIndex[rf.me] = len(rf.log) + rf.effectiveFirstIndex
		rf.matchIndex[rf.me] = len(rf.log) + rf.effectiveFirstIndex - 1
		index = len(rf.log) - 1 + rf.effectiveFirstIndex
		go rf.startAgreement(command, newLogEntry, index, rf.currentTerm)
	}

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
	// // fmt.Printf("Kill called for server %v\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.status) != nil ||
		e.Encode(rf.effectiveFirstIndex) != nil ||
		e.Encode(rf.snapshotLastIncludedTerm) != nil {
		log.Fatalf("Server %v (term = %v status = %v log = %v) persist failed", rf.me, rf.currentTerm, rf.status, rf.log)
	}
	// if rf.snapshot != nil {
	// 	if  {
	// 		log.Fatalf("Server %v (term = %v status = %v log = %v) persist snapshot metadata failed", rf.me, rf.currentTerm, rf.status, rf.log)
	// 	}
	// }
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
	// fmt.Printf("Server %v persisted its state and snapshot %v\n", rf.me, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var persistedCurrentTerm int
	var persistedVotedFor int
	var persistedLog []LogEntry
	var persistedStatus int
	var persistedEffectiveFirstIndex int
	var persistedSnapshotLastIncludedTerm int
	if d.Decode(&persistedCurrentTerm) != nil ||
		d.Decode(&persistedVotedFor) != nil ||
		d.Decode(&persistedLog) != nil ||
		d.Decode(&persistedStatus) != nil ||
		d.Decode(&persistedEffectiveFirstIndex) != nil ||
		d.Decode(&persistedSnapshotLastIncludedTerm) != nil {
		log.Fatalf("Server %v (term = %v status = %v log = %v) readPersist failed", rf.me, rf.currentTerm, rf.status, rf.log)
	} else {
		rf.currentTerm = persistedCurrentTerm
		rf.votedFor = persistedVotedFor
		rf.log = persistedLog
		rf.status = persistedStatus
		rf.effectiveFirstIndex = persistedEffectiveFirstIndex
		rf.snapshotLastIncludedTerm = persistedSnapshotLastIncludedTerm
		if rf.effectiveFirstIndex > 1 {
			rf.commitIndex = rf.effectiveFirstIndex - 1
			rf.lastApplied = rf.effectiveFirstIndex - 1
		}
		// fmt.Printf("Server %v after readPersist: currentTerm = %v votedFor = %v status = %v effectiveFirstIndex = %v snapshotLastIncludedTerm = %v commitIndex = %v lastApplied = %v log = %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.status, rf.effectiveFirstIndex, rf.snapshotLastIncludedTerm, rf.commitIndex, rf.lastApplied, rf.log)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index >= rf.effectiveFirstIndex {
		// Discards log entries up till index and updates effectiveFirstIndex and snapshotLastIncluded Term
		// fmt.Printf("Server %v (term = %v status = %v log = %v commitIndex = %v lastApplied = %v) called Snapshot(index = %v, snapshot = %v)\n", rf.me, rf.currentTerm, rf.status, rf.log, rf.commitIndex, rf.lastApplied, index, snapshot)
		rf.snapshotLastIncludedTerm = rf.log[index-rf.effectiveFirstIndex].Term
		currEffectiveFirstIndex := rf.effectiveFirstIndex
		rf.effectiveFirstIndex = index + 1
		var newLog = make([]LogEntry, len(rf.log)-(rf.effectiveFirstIndex-currEffectiveFirstIndex))
		// // fmt.Printf("Copying log entries %v through %v\n", rf.effectiveFirstIndex-currEffectiveFirstIndex, len(rf.log)-1)
		copy(newLog, rf.log[rf.effectiveFirstIndex-currEffectiveFirstIndex:])
		rf.log = newLog
		// // fmt.Printf("Server %v (term = %v status = %v effectiveFirstIndex = %v) new log after Snapshot %v\n", rf.me, rf.currentTerm, rf.status, rf.effectiveFirstIndex, rf.log)

		rf.snapshot = snapshot
		rf.persist()
	}
	// Ignore snapshots with index < rf.effectiveFirstIndex, an installed snapshot that covers more entries is being sent to the app
}

// RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // Candidate's term
	CandidateId  int // Candidate requesting vote
	LastLogIndex int // Log index of last candidate log entry
	LastLogTerm  int // Term of last candidate log entry
}

// RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // Receiver's currentTerm
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("Server %v (term = %v status = %v) got RequestVote for candidate %v (term = %v)\n", rf.me, rf.currentTerm, rf.status, args.CandidateId, args.Term)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		// Resets votedFor if a new higher term is received
		rf.votedFor = -1

		// If discovers a term larger than its own, updates currentTerm and reverts to a follower immediately
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.status = Follower
		rf.nextIndex = nil
		rf.matchIndex = nil
	}

	hasVotedDiffCand := rf.votedFor != -1 && rf.votedFor != args.CandidateId
	// Election restriction check
	lastLogTerm := rf.snapshotLastIncludedTerm
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	candLogAsUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.effectiveFirstIndex+len(rf.log)-1)
	// // fmt.Printf("Server %v hasVotedDiffCand = %v candLogAsUpToDate = %v\n", rf.me, hasVotedDiffCand, candLogAsUpToDate)
	if !hasVotedDiffCand && candLogAsUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.electionTimeoutStartTime = time.Now()
	}

	rf.persist()
	return
}

// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderId     int        // For followers to redirect clients
	PrevLogIndex int        // Log index of the entry immediately preceding new ones
	PrevLogTerm  int        // Term of PrevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat; may be >1 for efficiency)
	LeaderCommit int        // Leader's commitIndex
}

// field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int  // Receiver's currentTerm, for leader to update itself
	Success bool // true means follower contained entry matching PrevLogIndex and PrevLogTerm

	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) binarySearchConflictIndex(target int) int {
	start := 0
	end := len(rf.log) - 1

	for start <= end {
		mid := (start + end) / 2

		if rf.log[mid].Term < target {
			start = mid + 1
		} else if mid > 0 && rf.log[mid-1].Term >= target {
			end = mid - 1
		} else {
			return mid
		}
	}

	// Assumes that invoker should have a suitable ConflictIndex
	log.Fatalf("Server %v (status = %v term = %v) invoked binarySearchConflictIndex but did NOT find a suitable ConflictIndex", rf.me, rf.status, rf.currentTerm)
	return -1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// fmt.Printf("Server %v (term = %v status = %v commitIndex = %v log = %v len(log) = %v) received AppendEntries from leader %v (term = %v leaderCommit = %v prevLogIndex = %v, preLogTerm = %v, Entries = %v)\n", rf.me, rf.currentTerm, rf.status, rf.commitIndex, rf.log, len(rf.log), args.LeaderId, args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	// Resets election timeout after receiving an valid AppendEntries call
	rf.electionTimeoutStartTime = time.Now()
	defer rf.persist()

	// If a candidate receives AppendEntries from a new leader, also reverts to follower
	if args.Term > rf.currentTerm || rf.status == Candidate {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.status = Follower
		rf.nextIndex = nil
		rf.matchIndex = nil
	}

	// Performs a log consistency check based on both PrevLogIndex and PrevLogTerm
	if args.PrevLogIndex > len(rf.log)+rf.effectiveFirstIndex-1 {
		reply.ConflictIndex = len(rf.log) + rf.effectiveFirstIndex
		reply.ConflictTerm = -1 // Ensures that leader cannot find a term with ConflictTerm
		// // fmt.Printf("Server %v (term = %v status = %v log = %v) rejects AppendEntries from leader %v due to log inconsistency (ConflictIndex = %v ConflictTerm = %v)\n", rf.me, rf.currentTerm, rf.status, rf.log, args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
		return
	} else if args.PrevLogIndex >= rf.effectiveFirstIndex && args.PrevLogTerm != rf.log[args.PrevLogIndex-rf.effectiveFirstIndex].Term {
		reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.effectiveFirstIndex].Term
		reply.ConflictIndex = rf.binarySearchConflictIndex(reply.ConflictTerm) + rf.effectiveFirstIndex
		// // fmt.Printf("Server %v (term = %v status = %v log = %v) rejects AppendEntries from leader %v due to log inconsistency (ConflictIndex = %v ConflictTerm = %v)\n", rf.me, rf.currentTerm, rf.status, rf.log, args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
		return
	} else if args.PrevLogIndex == rf.effectiveFirstIndex-1 && args.PrevLogTerm != rf.snapshotLastIncludedTerm {
		log.Fatalf("Log consistency check: PrevLogTerm should match commited entries in the snapshot, args.PrevLogIndex (%v) rf.effectiveFirstIndex (%v), but args.PrevLogTerm (%v) != rf.snapshotLastIncludedTerm (%v)", args.PrevLogIndex, rf.effectiveFirstIndex, args.PrevLogTerm, rf.snapshotLastIncludedTerm)
	} else if args.PrevLogIndex < rf.effectiveFirstIndex-1 {
		// Assumes that request terms match terms of entries in the snapshot, since they're already committed
	}

	reply.Success = true

	for i, newEntry := range args.Entries {
		// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
		newEntryIndex := args.PrevLogIndex + i + 1
		if newEntryIndex >= rf.effectiveFirstIndex {
			if newEntryIndex <= len(rf.log)+rf.effectiveFirstIndex-1 && rf.log[newEntryIndex-rf.effectiveFirstIndex].Term != newEntry.Term {
				rf.log = rf.log[:newEntryIndex-rf.effectiveFirstIndex]
			}

			// Append new entries only if they're not alr in the log
			if newEntryIndex == len(rf.log)+rf.effectiveFirstIndex {
				rf.log = append(rf.log, newEntry)
			}
		}
	}
	// // fmt.Printf("Server %v (term = %v status = %v) log after AppendEntries %v\n", rf.me, rf.currentTerm, rf.status, rf.log)

	if args.LeaderCommit > rf.commitIndex {
		// Takes min(leaderCommit, index of last new entry)
		var newCommitIndex int
		if args.LeaderCommit <= args.PrevLogIndex+len(args.Entries) {
			newCommitIndex = args.LeaderCommit
		} else {
			newCommitIndex = args.PrevLogIndex + len(args.Entries)
		}

		// Ignores outdated smaller newCommitIndex
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			// // // fmt.Printf("Server %v (term = %v status = %v) stats after AppendEntries: commitIndex = %v\n", rf.me, rf.currentTerm, rf.status, rf.commitIndex)
			rf.addMsgsToApply()
		}
	}

	// // fmt.Printf("Server %v (term = %v status = %v) after processing AppendEntries from server %v (term = %v)\n", rf.me, rf.currentTerm, rf.status, args.LeaderId, args.Term)

	return
}

type InstallSnapshotArgs struct {
	Term              int    // Leader's term
	LeaderId          int    // So followers can redirect clients
	LastIncludedIndex int    // The snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // Term of LastCludedIndex
	Data              []byte // Raw bytes of the entire snapshot, since we're NOT implementing the offset mechanism
}

type InstallSnapshotReply struct {
	Term int // Current term, for leader to update itsefl
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("Server %v (term = %v status = %v effectiveFirstIndex = %v log = %v) installing snapshot (LastIncludedIndex = %v, LastIncludedTerm = %v)\n", rf.me, rf.currentTerm, rf.status, rf.effectiveFirstIndex, rf.log, args.LastIncludedIndex, args.LastIncludedTerm)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	defer rf.persist()

	if args.LastIncludedIndex >= rf.effectiveFirstIndex {
		// Discards any existing snapshot with a smaller index
		rf.snapshot = args.Data
		rf.snapshotLastIncludedTerm = args.LastIncludedTerm
		if rf.commitIndex < args.LastIncludedIndex {
			rf.commitIndex = args.LastIncludedIndex
		}

		if args.LastIncludedIndex < rf.effectiveFirstIndex+len(rf.log) {
			// If existing log entry has same index and term as snapshot's last included entry, retain log entries following it and reply
			logLastIncludedIndex := args.LastIncludedIndex - rf.effectiveFirstIndex
			var newLog = make([]LogEntry, len(rf.log)-(logLastIncludedIndex+1))
			copy(newLog, rf.log[logLastIncludedIndex+1:])
			rf.log = newLog

			rf.effectiveFirstIndex = args.LastIncludedIndex + 1
			if rf.lastApplied < args.LastIncludedIndex {
				rf.msgsToApply = append(rf.msgsToApply, ApplyMsg{SnapshotValid: true, Snapshot: rf.snapshot, SnapshotTerm: rf.snapshotLastIncludedTerm, SnapshotIndex: rf.effectiveFirstIndex - 1, CommandValid: false})
				rf.hasEntriesToApply.Broadcast()
				rf.lastApplied = args.LastIncludedIndex
			}
			return
		}

		// Discards the entire log
		rf.log = make([]LogEntry, 0)
		rf.effectiveFirstIndex = args.LastIncludedIndex + 1
		if rf.lastApplied < args.LastIncludedIndex {
			rf.lastApplied = args.LastIncludedIndex
		}

		// Resets state machine using snapshot contents
		// fmt.Printf("Server %v (term = %v status = %v effectiveFirstIndex = %v) new log after installing snapshot Case 2 %v\n", rf.me, rf.currentTerm, rf.status, rf.effectiveFirstIndex, rf.log)
		rf.msgsToApply = append(rf.msgsToApply, ApplyMsg{SnapshotValid: true, Snapshot: rf.snapshot, SnapshotTerm: rf.snapshotLastIncludedTerm, SnapshotIndex: rf.effectiveFirstIndex - 1, CommandValid: false})
		rf.hasEntriesToApply.Broadcast()
	}
}

func (rf *Raft) startElection(electionTimeout time.Duration) {
	rf.mu.Lock()

	// Starts an election if appropriate
	reachedElectionTimeout := (time.Now().Sub(rf.electionTimeoutStartTime))/time.Millisecond >= electionTimeout
	if reachedElectionTimeout && rf.status != Leader {
		rf.status = Candidate
		rf.currentTerm++
		// fmt.Printf("Election started from server %v, log = %v, rf.currentTerm is incremented to %v\n", rf.me, rf.log, rf.currentTerm)
		rf.votedFor = rf.me
		rf.persist()
		rf.electionTimeoutStartTime = time.Now()
		electionCh := make(chan RequestVoteReply)
		electionTerm := rf.currentTerm
		peersNum := len(rf.peers)

		for i := 0; i < peersNum; i++ {
			if i != rf.me {
				lastLogTerm := rf.snapshotLastIncludedTerm
				if len(rf.log) > 0 {
					lastLogTerm = rf.log[len(rf.log)-1].Term
				}
				args := RequestVoteArgs{Term: electionTerm, CandidateId: rf.me, LastLogIndex: len(rf.log) - 1 + rf.effectiveFirstIndex, LastLogTerm: lastLogTerm}
				// // fmt.Printf("Server %v is requesting a vote from server %v\n", rf.me, i)
				go func(i int, args RequestVoteArgs, electionCh chan RequestVoteReply) {
					rf.mu.Lock()
					deliveredTimely := false
					for !deliveredTimely && !rf.killed() {
						reply := RequestVoteReply{}
						rf.mu.Unlock()
						deliveredTimely = rf.peers[i].Call("Raft.RequestVote", &args, &reply)
						if deliveredTimely {
							electionCh <- reply
						}
						rf.mu.Lock()
					}
					rf.mu.Unlock()
				}(i, args, electionCh)

			}
		}
		rf.mu.Unlock()

		voteCount := 1
		// // fmt.Printf("Server %v is pendings votes\n", rf.me)
		for i := 0; i < peersNum-1; i++ {
			reply, ok := <-electionCh
			rf.mu.Lock()
			if ok == false {
				rf.mu.Unlock()
				log.Fatalf("electionCh is closed prematurely, i = %v", i)
			}
			if reply.Term > rf.currentTerm {
				// Aborts this election, updates currentTerm and reverts to follower if any receiver has a larger term number
				// // // // fmt.Printf("Server %v (status = %v) discovered a larger term num during election\n", rf.me, rf.status)
				rf.currentTerm = reply.Term
				rf.status = Follower
				rf.nextIndex = nil
				rf.matchIndex = nil
				rf.persist()
				rf.mu.Unlock()
				return
			} else if reply.Term != electionTerm {
				// Stops the election process when the server term has changed
				rf.mu.Unlock()
				return
			} else if reply.VoteGranted == true {
				voteCount++
			}

			// // // // fmt.Printf("Server %v current vote count = %v\n", rf.me, voteCount)
			if voteCount >= peersNum/2+1 {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		}
		rf.mu.Lock()
		// // fmt.Printf("Server %v got enough vote responses: voteCount = %v, electionTerm = %v, rf.currentTerm = %v, rf.status = %v\n", rf.me, voteCount, electionTerm, rf.currentTerm, rf.status)
		// A candidate becomes a leader only when it wins this round of election and the term number is still valid.
		if voteCount >= peersNum/2+1 && electionTerm == rf.currentTerm && rf.status == Candidate {
			// // fmt.Printf("Server %v (term = %v) is elected leader!\n", rf.me, rf.currentTerm)
			// Initializes leader state
			rf.status = Leader
			rf.persist()
			rf.nextIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log) + rf.effectiveFirstIndex
			}
			rf.matchIndex = make([]int, len(rf.peers))
			// Sends the first heartbeat right away
			rf.heartbeatIntervalStartTime = time.Now().Add(time.Duration(-heartbeatInterval) * time.Millisecond)

			// Starts heartbeating as leader
			go rf.heartbeatTicker()
		}
	}

	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeat(term int) {
	rf.mu.Lock()
	// fmt.Printf("Server %v sendHeartbeat gets lock\n", rf.me)

	if term != rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	// Sends heartbeats if the preset interval has passed
	if (time.Now().Sub(rf.heartbeatIntervalStartTime))/time.Millisecond >= time.Duration(heartbeatInterval) {
		peersNum := len(rf.peers)
		heartbeatCh := make(chan AppendEntriesReply)
		heartbeatTerm := rf.currentTerm
		rf.heartbeatIntervalStartTime = time.Now() // Resets heartbeat timeout timer

		for i := 0; i < peersNum; i++ {
			if i != rf.me {
				// Sends empty logs in heartbeat msgs
				// // fmt.Printf("Server %v (status = %v len(log) = %v) sends a heartbeat to server %v (term = %v nextIndex = %v)\n", rf.me, rf.status, len(rf.log), i, rf.currentTerm, rf.nextIndex[i])
				go func(i int, term int, eartbeatCh chan AppendEntriesReply) {
					rf.mu.Lock()

					if term != rf.currentTerm {
						rf.mu.Unlock()
						return
					}

					if !rf.updateFollowerWithSnapshot(i) {
						rf.mu.Unlock()
						return
					}

					prevLogIndex := rf.nextIndex[i] - 1
					var prevLogTerm int
					if prevLogIndex >= rf.effectiveFirstIndex {
						prevLogTerm = rf.log[prevLogIndex-rf.effectiveFirstIndex].Term
					} else if prevLogIndex == rf.effectiveFirstIndex-1 {
						prevLogTerm = rf.snapshotLastIncludedTerm
					} else {
						log.Fatalf("Server %v's nextIndex for server %v is too small\n", rf.me, i)
					}
					args := AppendEntriesArgs{Term: term, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: make([]LogEntry, 0), LeaderCommit: rf.commitIndex}

					deliveredTimely := false
					for !deliveredTimely && !rf.killed() && term == rf.currentTerm {
						reply := AppendEntriesReply{}
						rf.mu.Unlock()
						deliveredTimely = rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
						rf.mu.Lock()
						if deliveredTimely {
							// // fmt.Printf("Server %v gets a heartbeat response from server %v (rf.Term = %v reply.Term = %v)\n", rf.me, i, rf.currentTerm, reply.Term)
							rf.mu.Unlock()
							heartbeatCh <- reply
							rf.mu.Lock()
						}
						if !deliveredTimely {
							// // fmt.Printf("Server %v re-sends timed-out heartbeat to server %v (term = %v)\n", rf.me, i, args.Term)
						}
					}
					rf.mu.Unlock()
				}(i, rf.currentTerm, heartbeatCh)
			}
		}

		for i := 0; i < peersNum-1; i++ {
			rf.mu.Unlock()
			reply, ok := <-heartbeatCh
			if ok == false {
				log.Fatalf("heartbeatCh is closed prematurely, i = %v", i)
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				// Updates currentTerm and reverts to follower if any receiver has a larger term number
				rf.currentTerm = reply.Term
				rf.status = Follower
				rf.nextIndex = nil
				rf.matchIndex = nil
				rf.persist()
				rf.mu.Unlock()
				return
			} else if heartbeatTerm != rf.currentTerm {
				// Stops the heartbeat process when the server term has changed
				break
			}
		}
	}
	rf.mu.Unlock()
}

type AppendEntriesReplyWithReceiverIndex struct {
	index   int
	term    int
	success bool
}

// Returns the largest index in leader's log where Term = target; if no such value, returns -1
func (rf *Raft) binarySearchLeaderEntryWithConflictTerm(target int) int {
	start := 0
	end := len(rf.log) - 1

	for start <= end {
		mid := (start + end) / 2

		if mid+1 < len(rf.log) && rf.log[mid+1].Term <= target {
			start = mid + 1
		} else if rf.log[mid].Term > target {
			end = mid - 1
		} else {
			if rf.log[mid].Term == target {
				return mid
			}

			break
		}
	}

	return -1
}

// Returns false if failed to install snapshots on the follower, therefore the caller should stop op; returns true otherwise
func (rf *Raft) updateFollowerWithSnapshot(i int) bool {
	// fmt.Printf("Server %v i = %v nextIndex = %v effectiveFirstIndex = %v updateFollowerWithSnapshot\n", rf.me, i, rf.nextIndex, rf.effectiveFirstIndex)
	for rf.nextIndex[i] < rf.effectiveFirstIndex {
		args := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: rf.effectiveFirstIndex - 1, LastIncludedTerm: rf.snapshotLastIncludedTerm, Data: rf.snapshot}
		reply := InstallSnapshotReply{}
		rf.mu.Unlock()
		deliveredTimely := rf.peers[i].Call("Raft.InstallSnapshot", &args, &reply)

		rf.mu.Lock()
		if args.Term != rf.currentTerm {
			// Term changed while waiting for reply
			return false
		} else if reply.Term > rf.currentTerm {
			// Receiver has a larger term
			rf.currentTerm = reply.Term
			rf.status = Follower
			rf.nextIndex = nil
			rf.matchIndex = nil
			rf.persist()
			return false
		} else if !deliveredTimely {
			// // fmt.Printf("Server %v re-sends timed-out InstallSnapshot to server %v (term = %v)\n", rf.me, i, args.Term)
		} else {
			rf.nextIndex[i] = rf.effectiveFirstIndex
		}
		// // fmt.Printf("Server %v nextIndex = %v effectiveFirstIndex = %v updateFollowerWithSnapshot\n", rf.me, rf.nextIndex, rf.effectiveFirstIndex)
	}

	return true
}

func (rf *Raft) addMsgsToApply() {
	// Assumes rf holds rf.mu when calling

	// fmt.Printf("Server %v (status = %v term = %v effectiveFirstIndex = %v lastApplied = %v commitIndex = %v) called applyTicker\n", rf.me, rf.status, rf.currentTerm, applyEffectiveFirstIndex, rf.lastApplied, rf.commitIndex)
	for i := rf.lastApplied + 1; rf.killed() == false && i <= rf.commitIndex; i++ {
		rf.lastApplied = i
		cmd := rf.log[i-rf.effectiveFirstIndex].Command
		// fmt.Printf("Server %v (status = %v term = %v) applies entry %v to SM, new commitIndex = %v, new lastApplied = %v\n", rf.me, rf.status, rf.currentTerm, cmd, rf.commitIndex, rf.lastApplied)
		rf.msgsToApply = append(rf.msgsToApply, ApplyMsg{CommandValid: true, Command: cmd, CommandIndex: i})
	}
	rf.hasEntriesToApply.Broadcast()
}

func (rf *Raft) startAgreement(command interface{}, newLogEntry LogEntry, newLogEntryIndex int, expectedTerm int) {
	rf.mu.Lock()
	// fmt.Printf("Server %v (status = %v len(log) = %v) startAgreement gets lock for task %v\n", rf.me, rf.status, len(rf.log), command)

	// If terms change while waiting for the lock, abort agreement process
	if rf.currentTerm != expectedTerm {
		// // fmt.Printf("Server %v (term = %v) aborting agreement process due to term changes while waiting for the lock\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		return
	}

	peersNum := len(rf.peers)
	agreementCh := make(chan AppendEntriesReplyWithReceiverIndex)
	rf.heartbeatIntervalStartTime = time.Now() // Resets heartbeat timeout timer

	for i := 0; i < peersNum; i++ {
		if i != rf.me && newLogEntryIndex >= rf.nextIndex[i] {
			go func(i int, term int, newLogEntryIndex int, agreementCh chan AppendEntriesReplyWithReceiverIndex) {
				rf.mu.Lock()
				if term != rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				if !rf.updateFollowerWithSnapshot(i) {
					rf.mu.Unlock()
					return
				}

				// Sends all entries from nextIndex onwards
				newLogEntries := make([]LogEntry, 0)
				for i := rf.nextIndex[i]; i <= newLogEntryIndex; i++ {
					newLogEntries = append(newLogEntries, rf.log[i-rf.effectiveFirstIndex])
				}
				prevLogIndex := rf.nextIndex[i] - 1
				var prevLogTerm int
				if rf.nextIndex[i] > rf.effectiveFirstIndex {
					prevLogTerm = rf.log[rf.nextIndex[i]-1-rf.effectiveFirstIndex].Term
				} else if rf.nextIndex[i] == rf.effectiveFirstIndex {
					prevLogTerm = rf.snapshotLastIncludedTerm
				} else {
					log.Fatalf("Server %v during startAgreement: Expects prevLogTerm to be >= effectiveFirstIndex", rf.me)
				}
				args := AppendEntriesArgs{Term: term, LeaderId: rf.me, Entries: newLogEntries, LeaderCommit: rf.commitIndex, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm}

				deliveredTimely := false
				logsDiffer := true
				for (!deliveredTimely || logsDiffer) && !rf.killed() {
					currNextIndex := args.PrevLogIndex + 1
					reply := AppendEntriesReply{}
					rf.mu.Unlock()
					// // fmt.Printf("Server %v (term = %v) sends an AppendEntries (not heartbeat) to server %v (args = %v)\n", args.LeaderId, args.Term, i, args)
					deliveredTimely = rf.peers[i].Call("Raft.AppendEntries", &args, &reply)

					rf.mu.Lock()
					// if rf.status != Leader {
					// 	rf.mu.Unlock()
					// 	agreementCh <- AppendEntriesReplyWithReceiverIndex{term: reply.Term, success: reply.Success, index: -1}
					// 	return
					// }
					if deliveredTimely {
						// // fmt.Printf("Server %v (when sending, term = %v prevLogIndex = %v prevLogTerm = %v) gets an AppendEntries response from server %v (reply = %v)\n", rf.me, args.Term, args.PrevLogIndex, args.PrevLogTerm, i, reply)

						if reply.Term <= args.Term && !reply.Success && rf.status == Leader {
							// Handles the case where leader and follower logs differ

							// Accelerated log backtracking
							if reply.ConflictTerm == -1 {
								// Case 1: Follower's log is too short
								currNextIndex = reply.ConflictIndex
							} else {
								largestIndexWithConflictTerm := rf.binarySearchLeaderEntryWithConflictTerm(reply.ConflictTerm)
								// // fmt.Printf("Server %v gets %v from binarySearchLeaderEntryWithConflictTerm, ConflictTerm = %v log = %v\n", rf.me, largestIndexWithConflictTerm, reply.ConflictTerm, rf.log)
								if largestIndexWithConflictTerm == -1 {
									// Case 2: Leader has NO entries with reply.ConflictTerm
									currNextIndex = reply.ConflictIndex
								} else {
									// Case 3: Leader has entries with reply.ConflictTerm
									currNextIndex = largestIndexWithConflictTerm + 1
								}
							}

							// Checks for concurrent modifications
							if rf.nextIndex[i] > currNextIndex {
								rf.nextIndex[i] = currNextIndex
							}

							if !rf.updateFollowerWithSnapshot(i) {
								rf.mu.Unlock()
								return
							}

							// Prepares retry args
							newLogEntries := make([]LogEntry, 0)
							for i := rf.nextIndex[i]; i <= newLogEntryIndex; i++ {
								newLogEntries = append(newLogEntries, rf.log[i-rf.effectiveFirstIndex])
							}
							args.Entries = newLogEntries
							args.PrevLogIndex = rf.nextIndex[i] - 1
							if args.PrevLogIndex >= rf.effectiveFirstIndex {
								args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.effectiveFirstIndex].Term
							} else if args.PrevLogIndex == rf.effectiveFirstIndex-1 {
								args.PrevLogTerm = rf.snapshotLastIncludedTerm
							} else {
								log.Fatalf("Server %v during retry: Expects PrevLogIndex to be >= effectiveFirstIndex - 1", rf.me)
							}
							// // fmt.Printf("Server %v retries AppendEntries (prevLogIndex = %v prevLogTerm = %v) to server %v (rf.currentTerm = %v args = %v)\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, i, rf.currentTerm, args)
						} else {
							// Successful replies, higher-term replies, or current server is no longer leader replies are sent to waiting thread for processing
							rf.mu.Unlock()
							logsDiffer = false
							agreementCh <- AppendEntriesReplyWithReceiverIndex{term: reply.Term, success: reply.Success, index: i}
							rf.mu.Lock()
						}
					} else {
						// // fmt.Printf("Server %v re-sends timed-out AppendEntries to server %v (term = %v)\n", rf.me, i, args.Term)
					}
				}

				rf.mu.Unlock()
			}(i, rf.currentTerm, newLogEntryIndex, agreementCh)
		}
	}

	for i := 0; i < peersNum-1; i++ {
		rf.mu.Unlock()
		reply, ok := <-agreementCh
		// // fmt.Printf("Server %v receives AppendEntries reply from server %v (term = %v success = %v \n", rf.me, reply.index, reply.term, reply.success)
		if ok == false {
			log.Fatalf("Server %v (status = %v)'s agreementCh is closed prematurely, i = %v", rf.me, rf.status, i)
		}

		rf.mu.Lock()

		// Updates currentTerm and reverts to follower if any receiver has a larger term number
		if reply.term > rf.currentTerm {
			rf.currentTerm = reply.term
			rf.status = Follower
			rf.nextIndex = nil
			rf.matchIndex = nil
			rf.persist()
			rf.mu.Unlock()
			return
		}

		// Stops the agreement process when the server term has changed
		if expectedTerm != rf.currentTerm || !reply.success {
			// // fmt.Printf("Server %v agreement process stoped due to term change\n", rf.me)
			break
		}

		// // fmt.Printf("Server %v nextIndex = %v matchIndex = %v commitIndex = %v before update\n", rf.me, rf.nextIndex, rf.matchIndex, rf.commitIndex)
		if newLogEntryIndex+1 > rf.nextIndex[reply.index] {
			rf.nextIndex[reply.index] = newLogEntryIndex + 1
		}
		if newLogEntryIndex > rf.matchIndex[reply.index] {
			rf.matchIndex[reply.index] = newLogEntryIndex

			// Updates leader's rf.commitIndex based on updated rf.matchIndex
			newCommitIndex := rf.commitIndex
			for i := newLogEntryIndex; i > rf.commitIndex; i-- {
				logEntryMatchCounter := 0
				for _, mi := range rf.matchIndex {
					if mi >= i {
						logEntryMatchCounter++
					}
				}
				if i < rf.effectiveFirstIndex {
					log.Fatalf("Server %v (status = %v term = %v log = %v effectiveFirstIndex = %v): Expects uncommitted index %v to NOT be in the snapshot", rf.me, rf.status, rf.currentTerm, rf.log, rf.effectiveFirstIndex, i)
				}
				if logEntryMatchCounter >= peersNum/2+1 && rf.log[i-rf.effectiveFirstIndex].Term == rf.currentTerm {
					newCommitIndex = i
					break
				}
			}
			// fmt.Printf("Server %v nextIndex = %v matchIndex = %v commitIndex = %v after update\n", rf.me, rf.nextIndex, rf.matchIndex, rf.commitIndex)

			if newCommitIndex > rf.commitIndex {
				rf.commitIndex = newCommitIndex
				// fmt.Printf("Server %v considers entry %v committed, log = %v, commitIndex = %v, lastApplied = %v, nextIndex = %v, matchIndex = %v\n", rf.me, newLogEntry, rf.log, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex)
				rf.addMsgsToApply()
			}
		}
	}

	rf.mu.Unlock()
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		// Random election timeouts.
		electionTimeoutInMs := time.Duration(electionTimeoutLowerBound + (rand.Int63() % electionTimeoutVariablePortion))
		go rf.startElection(electionTimeoutInMs)

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		// fmt.Printf("Server %v electionTicker ticked (currentTerm = %v, dead = %v, status = %v)...\n", rf.me, rf.currentTerm, rf.killed(), rf.status)
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeatTicker() {
	// // fmt.Printf("Server %v heartbeatTicker starts...\n", rf.me)
	rf.mu.Lock()
	// // fmt.Printf("Server %v heartbeatTicker gets lock...\n", rf.me)
	for rf.killed() == false && rf.status == Leader {
		// // fmt.Printf("Server %v heartbeatTicker ticked (status = %v currentTerm = %v)...\n", rf.me, rf.status, rf.currentTerm)
		go rf.sendHeartbeat(rf.currentTerm)

		// pause to wait for next heartbeat.
		rf.mu.Unlock()
		time.Sleep(time.Duration(heartbeatInterval) * time.Millisecond)

		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

func (rf *Raft) applyTicker() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.killed() == false {
		// Waits till there are committed entries that aren't applied on this server
		for len(rf.msgsToApply) == 0 {
			rf.hasEntriesToApply.Wait()
		}

		// applyEffectiveFirstIndex := rf.effectiveFirstIndex
		// fmt.Printf("Server %v (status = %v term = %v effectiveFirstIndex = %v lastApplied = %v commitIndex = %v) called applyTicker\n", rf.me, rf.status, rf.currentTerm, rf.effectiveFirstIndex, rf.lastApplied, rf.commitIndex)
		l := len(rf.msgsToApply)
		for i := 0; i < l; i++ {
			// rf.lastApplied = i
			// cmd := rf.log[i-rf.effectiveFirstIndex].Command
			msg := rf.msgsToApply[0]
			newMsgsToApply := make([]ApplyMsg, len(rf.msgsToApply)-1)
			copy(newMsgsToApply, rf.msgsToApply[1:])
			rf.msgsToApply = newMsgsToApply
			// fmt.Printf("Server %v (status = %v term = %v) applies message %v to SM, new commitIndex = %v, new lastApplied = %v\n", rf.me, rf.status, rf.currentTerm, msg, rf.commitIndex, rf.lastApplied)
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
	}
}
