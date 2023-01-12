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
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

func max(a int, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

func min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

// each log entry contains command for state machine, and term when entry was received by leader (first index is 1)
type LogEntry struct {
	Command      interface{}
	ReceivedTerm int
}

const (
	Follower  = 1
	Candidate = 2
	Leader    = 3
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role            int // Follower, Candidate or Leader
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	// persistent state (Updated on stable storage before responding to RPCs)
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// volatile state
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state on leaders (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	applyCh chan ApplyMsg
}

func (rf *Raft) ResetElectionTimer() {
	rf.electionTimer.Reset(time.Duration(rand.Intn(150)+300) * time.Millisecond)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.role == Leader)
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("readPersist Decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// Invoked by leader to replicate log entries (§5.3); also used as
// heartbeat (§5.2).
type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat;
	//may send more than one for efficiency)
	LeaderCommit int // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int // currentTerm, for leader to update itself
	Append  bool
	Success bool //true if follower contained entry matching
	//prevLogIndex and prevLogTerm
}

// an AppendEntries RPC handler method that resets the election timeout so that
// other servers don't step forward as leaders when one has already been elected.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("AppendEntries to server %d leadercommit %d rf.commitindex %d\n", rf.me, args.LeaderCommit, rf.commitIndex)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Append = false
		return
		// fmt.Printf("server %d update currentTerm %d\n", rf.me, rf.currentTerm)
	} else {
		rf.role = Follower
		rf.ResetElectionTimer()
		rf.currentTerm = args.Term
		if args.PrevLogIndex < 0 {
			reply.Append = false
			return
		} else if len(rf.log) < args.PrevLogIndex+1 || rf.log[args.PrevLogIndex].ReceivedTerm != args.PrevLogTerm {
			reply.Append = true
			reply.Success = false
			// fmt.Printf("responding len(rf.log) %d args.PrevLogIndex %d rf.log[args.PrevLogIndex].ReceivedTerm %d args.PrevLogTerm %d\n", len(rf.log), args.PrevLogIndex, rf.log[args.PrevLogIndex].ReceivedTerm, args.PrevLogTerm)
			// fmt.Printf("server %d is responding to AppendEntries fail here\n", rf.me)
			return
		}
		reply.Append = true
		// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		for idx, entry := range args.Entries {
			currentIdx := args.PrevLogIndex + 1 + idx
			if currentIdx < len(rf.log) && rf.log[currentIdx] != entry {
				rf.log = rf.log[:currentIdx]
			} else if currentIdx < len(rf.log) && rf.log[currentIdx] == entry {
				continue
			}
			// fmt.Printf("Before: server %d len(rf.log) %d\n", rf.me, len(rf.log))
			rf.log = append(rf.log, entry)
			// fmt.Printf("After: server %d len(rf.log) %d\n", rf.me, len(rf.log))
		}
		rf.persist()
		{
			reply.Success = true
			// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
			if args.LeaderCommit > rf.commitIndex {
				lastNewEntryIndex := len(rf.log) - 1
				rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
				// fmt.Printf("Updating server %d commitIndex to %d from leader\n", rf.me, rf.commitIndex)
				// fmt.Printf("command %v\n", rf.log[rf.commitIndex].Command)
				for rf.commitIndex > rf.lastApplied {
					rf.lastApplied++
					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      rf.log[rf.lastApplied].Command,
						CommandIndex: rf.lastApplied,
					}
					rf.applyCh <- applyMsg
				}
			}
		}

	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.role = Follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.Term = args.Term
	}
	// fmt.Printf("Ser %d rf.votedFor %d len(rf.log) %d args.LastLogTerm %d rf.log[len(rf.log)-1].ReceivedTerm %d args.LastLogIndex %d\n", rf.me, rf.votedFor, len(rf.log), args.LastLogTerm, rf.log[len(rf.log)-1].ReceivedTerm, args.LastLogIndex)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].ReceivedTerm ||
			args.LastLogTerm == rf.log[len(rf.log)-1].ReceivedTerm && args.LastLogIndex >= len(rf.log)-1 {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.ResetElectionTimer()
			rf.currentTerm = args.Term
		}
	}
	rf.persist()

	// fmt.Printf("IncomingTerm %d ServerTerm %d Server %d is %v VoteGranted is %v Candidate is %d\n", args.Term, rf.currentTerm, rf.me, rf.role, reply.VoteGranted, args.CandidateId)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// fmt.Printf("Entering Start function\n")
	index := len(rf.log) - 1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = (rf.role == Leader)
	term = rf.currentTerm
	if isLeader {
		rf.log = append(rf.log, LogEntry{command, term})
		index = len(rf.log) - 1
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = index
			rf.matchIndex[i] = 0
		}
		rf.matchIndex[rf.me] = index
		rf.persist()
		rf.BroadcastAppendEntries()
	}
	// fmt.Printf("server %d Start function return index %d term %d isLeader %v\n", rf.me, index, term, isLeader)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) BroadcastAppendEntries() {

	lastLogIndex := len(rf.log) - 1
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
			go func(i int) {
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				// keep updating this as well
				args.PrevLogIndex = rf.nextIndex[i] - 1
				if args.PrevLogIndex <= -1 {
					args.PrevLogTerm = -1
				} else {
					args.PrevLogTerm = rf.log[rf.nextIndex[i]-1].ReceivedTerm
				}
				entries := []LogEntry{}
				if rf.nextIndex[i] < len(rf.log) {
					entries = rf.log[rf.nextIndex[i]:]
				}
				args.Entries = make([]LogEntry, len(entries))
				copy(args.Entries, entries)
				args.LeaderCommit = rf.commitIndex
				reply := AppendEntriesReply{}
				// go func(i int) {
				ok := rf.sendAppendEntries(i, &args, &reply)
				// If successful: update nextIndex and matchIndex for follower (§5.3)
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Append {
					if reply.Success == true {
						rf.nextIndex[i] = lastLogIndex + 1
						rf.matchIndex[i] = lastLogIndex
						// fmt.Printf("leader %d appendReceived++ success\n", rf.me)
						// fmt.Printf("index %d Raft server %d sending out AppendEntries to peer %d succeeds rf.nextIndex[i] %d\n", index, rf.me, i, rf.nextIndex[i])
					} else { // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
						rf.nextIndex[i] = max(1, rf.nextIndex[i]-1)
						// fmt.Printf("leader %d appendReceived++ fail\n", rf.me)
						// fmt.Printf("index %d Raft server %d sending out AppendEntries to peer %d fails rf.nextIndex[i] %d\n", index, rf.me, i, rf.nextIndex[i])
					}
				}
			}(i)

		}
	}
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	for N := lastLogIndex; N > rf.commitIndex; N-- {
		numLogs := 1
		if rf.log[N].ReceivedTerm == rf.currentTerm {
			for j := 0; j < len(rf.peers); j++ {
				if j != rf.me {
					if rf.matchIndex[j] >= N {
						numLogs++
					}
				}
			}
			if numLogs > len(rf.peers)/2 {
				rf.commitIndex = N
			}
		}
	}
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ResetElectionTimer()
			if rf.role == Leader { // Do not start election on leader
				rf.mu.Unlock()
				break
			}
			rf.role = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			numVotes := 1
			voteReceived := 1
			voteResultChan := make(chan bool)

			lastLogIndex := len(rf.log) - 1
			lastLogTerm := -1
			if lastLogIndex != -1 {
				lastLogTerm = rf.log[lastLogIndex].ReceivedTerm
			}
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			// fmt.Printf("Raft server %d is candidate, current term %d\n", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			// create a background goroutine that will kick off leader election periodically
			// by sending out RequestVote RPCs when it hasn't heard back from another peer for a while
			// issues RequestVote RPCs in parallel to each of the other servers in the cluster.
			if rf.killed() == false && rf.role == Candidate {
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						reply := RequestVoteReply{}
						// fmt.Printf("Raft server %d sending out RequestVote\n", rf.me)
						go func(i int) {
							ok := rf.sendRequestVote(i, &args, &reply)
							if ok {
								voteResultChan <- reply.VoteGranted
							} else {
								voteResultChan <- false
							}
						}(i)
						if reply.Term > rf.currentTerm {
							rf.role = Follower
							rf.votedFor = -1
							rf.currentTerm = reply.Term
							rf.persist()
							return
						}
					}
				}
				for {
					result := <-voteResultChan
					voteReceived++
					if result {
						numVotes++
					}
					if numVotes > len(rf.peers)/2 {
						break
					}
					if voteReceived >= len(rf.peers) {
						break
					}
				}
				// wg.Wait()
				// (a) wins the election
				// fmt.Printf("Term %d Total of %d servers\n", rf.currentTerm, len(rf.peers))
				// fmt.Printf("Term %d Raft server %d has %d numVotes\n", rf.currentTerm, rf.me, numVotes)

				// if state changed during election, ignore the couting
				rf.mu.Lock()
				if rf.role != Candidate {
					DPrintf("Server %v is no longer candidate", rf.me)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				if numVotes >= len(rf.peers)/2+1 {
					rf.mu.Lock()
					rf.role = Leader
					rf.ResetElectionTimer()
					rf.mu.Unlock()
					// fmt.Printf("Server %d is leader now!\n", rf.me)
					rf.BroadcastAppendEntries()
				}
				rf.persist()
			}
		case <-rf.heartbeatTicker.C:
			rf.mu.Lock()
			state := rf.role
			rf.mu.Unlock()
			if state == Leader {
				rf.BroadcastAppendEntries()
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rand.Seed(time.Now().UnixNano())

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1 // hasn't granted vote to candidate
	// Raft logs are 1-indexed; add a dummy entry in the first slot to enforce this
	dummyEntry := LogEntry{
		Command:      0,
		ReceivedTerm: -1,
	}
	rf.log = append(rf.log, dummyEntry)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(rf.peers))

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.electionTimer = time.NewTimer(time.Duration(rand.Intn(150)+300) * time.Millisecond)
	rf.heartbeatTicker = time.NewTicker(time.Duration(150) * time.Millisecond)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
