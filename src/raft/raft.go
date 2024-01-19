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
	// "bytes"

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "6.5840/labgob"
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
	mu        *sync.Mutex         // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	status    int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm              int        // Latest term server has seen (initialized as 0 and increases monotonically)
	votedFor                 int        // CandidateId that received vote in current term (-1 if none)
	commitIndex              int        // Highest index of committed log entry (initialized as 0 and increases monotonically)
	log                      []LogEntry // Log entries
	electionTimeoutStartTime time.Time  // Last time server received AppendEntries from current leader or granted vote to candidate

	// Leader states
	nextIndex                  []int     // For each server, index of next entry to send to that server (initialized as leader's last log index + 1)
	matchIndex                 []int     // For each server, highest index of replicated entries (initialized as 0 and increases monotonically)
	heartbeatIntervalStartTime time.Time // Last time the leader sent an AppendEntries call to followers
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
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // Candidate's term
	CandidateId  int // Candidate requesting vote
	LastLogIndex int // Log index of last candidate log entry
	LastLogTerm  int // Term of last candidate log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // Receiver's currentTerm
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("Server %v (term = %v status = %v) got RequestVote for candidate %v (term = %v)\n", rf.me, rf.currentTerm, rf.status, args.CandidateId, args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	reply.VoteGranted = false

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
	candLogAsUpToDate := args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)
	// fmt.Printf("Server %v hasVotedDiffCand = %v candLogAsUpToDate = %v\n", rf.me, hasVotedDiffCand, candLogAsUpToDate)
	if !hasVotedDiffCand && candLogAsUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.electionTimeoutStartTime = time.Now()
	}

	return
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

// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term     int // Leader's term
	LeaderId int // For followers to redirect clients
	// PrevLogIndex int // Log index of the entry immediately preceding new ones
	// PrevLogTerm  int // Term of PrevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat; may be >1 for efficiency)
	LeaderCommit int        // Leader's commitIndex
}

// field names must start with capital letters!
type AppendEntriesReply struct {
	Term int // Receiver's currentTerm, for leader to update itself
	// Success bool // true means follower contained entry matching PrevLogIndex and PrevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("Server %v (term = %v status = %v) received AppendEntries from leader %v (term = %v)\n", rf.me, rf.currentTerm, rf.status, args.LeaderId, args.Term)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// If a candidate receives AppendEntries from a new leader, also reverts to follower
	if args.Term > rf.currentTerm || rf.status == Candidate {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.status = Follower
		rf.nextIndex = nil
		rf.matchIndex = nil
	}

	// Resets election timeout after receiving an valid AppendEntries call
	rf.electionTimeoutStartTime = time.Now()
	// fmt.Printf("Server %v (term = %v status = %v) after processing AppendEntries from server %v (term = %v)\n", rf.me, rf.currentTerm, rf.status, args.LeaderId, args.Term)

	return
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	// fmt.Printf("Server %v sendHeartbeat gets lock\n", rf.me)

	// Sends heartbeats if the preset interval has passed
	if (time.Now().Sub(rf.heartbeatIntervalStartTime))/time.Millisecond >= time.Duration(heartbeatInterval) {
		peersNum := len(rf.peers)
		heartbeatCh := make(chan AppendEntriesReply)
		rf.heartbeatIntervalStartTime = time.Now() // Resets heartbeat timeout timer

		for i := 0; i < peersNum; i++ {
			if i != rf.me {
				// Sends empty logs in heartbeat msgs
				args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, Entries: make([]LogEntry, 0), LeaderCommit: rf.commitIndex}
				reply := AppendEntriesReply{}
				go func(i int, args AppendEntriesArgs, reply AppendEntriesReply, heartbeatCh chan AppendEntriesReply) {
					rf.mu.Lock()
					deliveredTimely := false
					for !deliveredTimely && !rf.killed() {
						// fmt.Printf("Server %v (status = 2) sends a heartbeat to server %v (term = %v)\n", args.LeaderId, i, args.Term)
						rf.mu.Unlock()
						deliveredTimely = rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
						rf.mu.Lock()
						if deliveredTimely {
							// fmt.Printf("Server %v gets a heartbeat response from server %v (rf.Term = %v reply.Term = %v)\n", rf.me, i, rf.currentTerm, reply.Term)
							rf.mu.Unlock()
							heartbeatCh <- reply
							rf.mu.Lock()
						}
						if !deliveredTimely {
							// fmt.Printf("Server %v re-sends timed-out heartbeat to server %v (term = %v)\n", rf.me, i, args.Term)
						}
					}
					rf.mu.Unlock()
				}(i, args, reply, heartbeatCh)
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
				rf.mu.Unlock()
				return
			}
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) heartbeatTicker() {
	// fmt.Printf("Server %v heartbeatTicker starts...\n", rf.me)
	rf.mu.Lock()
	// fmt.Printf("Server %v heartbeatTicker gets lock...\n", rf.me)
	for rf.killed() == false && rf.status == Leader {
		// fmt.Printf("Server %v heartbeatTicker ticked (status = %v currentTerm = %v)...\n", rf.me, rf.status, rf.currentTerm)
		rf.mu.Unlock()
		go rf.sendHeartbeat()

		// pause to wait for next heartbeat.
		time.Sleep(time.Duration(heartbeatInterval) * time.Millisecond)

		rf.mu.Lock()
	}
	rf.mu.Unlock()
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

	// Your code here (2B).

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
	// fmt.Printf("Kill called for server %v\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// // example RequestVote RPC arguments structure.
// // field names must start with capital letters!
// type RequestVoteArgs struct {
// 	// Your data here (2A, 2B).
// 	Term         int // Candidate's term
// 	CandidateId  int // Candidate requesting vote
// 	LastLogIndex int // Log index of last candidate log entry
// 	LastLogTerm  int // Term of last candidate log entry
// }

// // example RequestVote RPC reply structure.
// // field names must start with capital letters!
//
//	type RequestVoteReply struct {
//		// Your data here (2A).
//		Term        int  // Receiver's currentTerm
//		VoteGranted bool // true means candidate received vote
//	}
func (rf *Raft) startElection(electionTimeout time.Duration) {
	rf.mu.Lock()

	// Starts an election if appropriate
	reachedElectionTimeout := (time.Now().Sub(rf.electionTimeoutStartTime))/time.Millisecond >= electionTimeout
	if reachedElectionTimeout && rf.status != Leader {
		rf.status = Candidate
		rf.currentTerm++
		// fmt.Printf("Election started from server %v, rf.currentTerm is incremented to %v\n", rf.me, rf.currentTerm)
		rf.votedFor = rf.me
		rf.electionTimeoutStartTime = time.Now()
		electionCh := make(chan RequestVoteReply)
		electionTerm := rf.currentTerm
		peersNum := len(rf.peers)

		for i := 0; i < peersNum; i++ {
			if i != rf.me {
				args := RequestVoteArgs{Term: electionTerm, CandidateId: rf.me, LastLogIndex: len(rf.log) - 1, LastLogTerm: rf.log[len(rf.log)-1].Term}
				reply := RequestVoteReply{}
				// fmt.Printf("Server %v is requesting a vote from server %v\n", rf.me, i)
				go func(i int, args RequestVoteArgs, reply RequestVoteReply, electionCh chan RequestVoteReply) {
					rf.mu.Lock()
					deliveredTimely := false
					for !deliveredTimely && !rf.killed() {
						rf.mu.Unlock()
						deliveredTimely = rf.peers[i].Call("Raft.RequestVote", &args, &reply)
						if deliveredTimely {
							electionCh <- reply
						}
						rf.mu.Lock()
					}
					rf.mu.Unlock()
				}(i, args, reply, electionCh)

			}
		}
		rf.mu.Unlock()

		voteCount := 1
		// fmt.Printf("Server %v is pendings votes\n", rf.me)
		for i := 0; i < peersNum-1; i++ {
			reply, ok := <-electionCh
			rf.mu.Lock()
			if ok == false {
				rf.mu.Unlock()
				log.Fatalf("electionCh is closed prematurely, i = %v", i)
			}
			if reply.VoteGranted == true {
				voteCount++
			} else if reply.Term > rf.currentTerm {
				// Aborts this election, updates currentTerm and reverts to follower if any receiver has a larger term number
				// fmt.Printf("Server %v (status = %v) discovered a larger term num during election\n", rf.me, rf.status)
				rf.currentTerm = reply.Term
				rf.status = Follower
				rf.nextIndex = nil
				rf.matchIndex = nil
				rf.mu.Unlock()
				return
			}

			// fmt.Printf("Server %v current vote count = %v\n", rf.me, voteCount)
			if voteCount >= peersNum/2+1 {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		}
		rf.mu.Lock()
		// fmt.Printf("Server %v got enough vote responses: voteCount = %v, electionTerm = %v, rf.currentTerm = %v, rf.status = %v\n", rf.me, voteCount, electionTerm, rf.currentTerm, rf.status)
		// A candidate becomes a leader only when it wins this round of election and the term number is still valid.
		if voteCount >= peersNum/2+1 && electionTerm == rf.currentTerm && rf.status == Candidate {
			// fmt.Printf("Server %v is elected leader!\n", rf.me)
			// Initializes leader state
			rf.status = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log)
			}
			rf.matchIndex = make([]int, len(rf.peers))
			// Sends the first heartbeat right away
			rf.heartbeatIntervalStartTime = time.Now().Add(time.Duration(-heartbeatInterval) * time.Millisecond)

			// Starts heartbeating as leader
			go rf.heartbeatTicker()
		}
	}

	// fmt.Printf("Server %v election ends...\n", rf.me)
	rf.mu.Unlock()
	// fmt.Printf("Server %v startElection releases lock...\n", rf.me)
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
	srv := labrpc.MakeServer()
	svc := labrpc.MakeService(rf)
	srv.AddService(svc)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0}) // Appends the first log entry for initial RPCs to have valid fields
	rf.nextIndex = nil
	rf.matchIndex = nil
	var mu sync.Mutex
	rf.mu = &mu
	rf.electionTimeoutStartTime = time.Now() // Starts a fresh election timeout clock
	rf.status = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	return rf
}
