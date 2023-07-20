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
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	// "6.824/labgob"
	"6.824/labrpc"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 1000
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

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

type State string
const (
	Follower State = "follower"
	Candidate = "candidate"
	Leader    = "leader"
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

	role            State // Follower, Candidate or Leader
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

	// currentLeader int
}

func (rf *Raft) ResetElectionTimer() {
	rf.electionTimer.Reset(time.Duration(rand.Intn(150)+150) * time.Millisecond)
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// rf.mu.Lock()
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.votedFor)
	// e.Encode(rf.commitIndex)
	// e.Encode(rf.log)
	// rf.mu.Unlock()
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var currentTerm int
	// var votedFor int
	// var commitIndex int
	// var log []LogEntry
	// if d.Decode(&currentTerm) != nil ||
	// 	d.Decode(&votedFor) != nil ||
	// 	d.Decode(&commitIndex) != nil ||
	// 	d.Decode(&log) != nil {
	// 	fmt.Printf("readPersist Decode error")
	// } else {
	// 	rf.currentTerm = currentTerm
	// 	rf.votedFor = votedFor
	// 	rf.commitIndex = commitIndex
	// 	rf.log = log
	// }
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
	
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	if args.Term < reply.Term {
		reply.Append = false
		return
	} else {
		rf.mu.Lock()
		Debug(dLog, "S%d: Term is higher, updating (%d > %d)", rf.me, args.Term, rf.currentTerm)
		
		rf.role = Follower // TODO(data race)
		rf.ResetElectionTimer()
		rf.currentTerm = args.Term
		rf.mu.Unlock()
		if args.PrevLogIndex < 0 {
			reply.Append = true
			return
		} else if len(rf.log) < args.PrevLogIndex+1 || rf.log[args.PrevLogIndex].ReceivedTerm != args.PrevLogTerm {
			reply.Append = true
			reply.Success = false
			return
		}
		Debug(dLog, "S%d -> S%d Sending PLI: %d PLT: %d N: %d LC: %d - %v", args.LeaderId, rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.nextIndex[rf.me], args.LeaderCommit, args.Entries)
		reply.Append = true
		// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		for idx, entry := range args.Entries {
			currentIdx := args.PrevLogIndex + 1 + idx
			if currentIdx < len(rf.log) && rf.log[currentIdx] != entry {
				rf.log = rf.log[:currentIdx]
			} else if currentIdx < len(rf.log) && rf.log[currentIdx] == entry {
				continue
			}
			Debug(dLog, "S%d -> S%d Before append: %v", args.LeaderId, rf.me, rf.log)
			rf.log = append(rf.log, entry)
			Debug(dLog, "S%d -> S%d After append: %v", args.LeaderId, rf.me, rf.log)
		}
		rf.persist()
		{
			reply.Success = true
			// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
			if args.LeaderCommit > rf.commitIndex {
				lastNewEntryIndex := len(rf.log) - 1
				rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
				Debug(dLog, "S%d Updating commitIndex to %d from leader", rf.me, rf.commitIndex)
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
	

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.mu.Unlock()

	// If the candidate's term is lower than the current term, reject the vote.
	if args.Term < rf.currentTerm {
		return
	}

	// If the candidate's term is higher than the current term, update the current term and transition to follower.
	if args.Term > rf.currentTerm {
		Debug(dLog, "S%d: Term is higher, updating (%d > %d)", rf.me, args.Term, rf.currentTerm)
		rf.mu.Lock()
		rf.role = Follower // TODO
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.mu.Unlock()
	}

	//  if the server hasn’t voted or the server has voted for this candidate (in case the RPC response didn’t reach to the candidate and RPC was retried)
	rf.mu.Lock()
	currentVoted := rf.votedFor
	lastIndex := len(rf.log)-1
	lastReceivedTerm := rf.log[len(rf.log)-1].ReceivedTerm
	rf.mu.Unlock()

	if currentVoted == -1 || currentVoted == args.CandidateId {
		if args.LastLogTerm > lastReceivedTerm ||
			args.LastLogTerm == lastReceivedTerm && args.LastLogIndex >= lastIndex {
			rf.mu.Lock()
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			// rf.ResetElectionTimer()
			rf.currentTerm = args.Term
			Debug(dVote, "S%d Granting Vote to S%d at T%d", rf.me, args.CandidateId, args.Term)
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			Debug(dVote, "S%d Is NOT Granting Vote to S%d at T%d, because candidate is not as least up to date", rf.me, args.CandidateId, args.Term)
			rf.mu.Unlock()
		}
	}
	// Debug(dPersist, "S%d Logs: %v", rf.peers[i].)
	rf.persist()
	rf.mu.Lock()
	Debug(dLog, "S%d is %v IncomingTerm %d ServerTerm %d VoteGranted is %v Candidate is %d", rf.me, rf.role, args.Term, rf.currentTerm, reply.VoteGranted, args.CandidateId)
	rf.mu.Unlock()
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
	
	isLeader = (rf.role == Leader)
	term = rf.currentTerm
	rf.mu.Unlock()
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
	Debug(dLog, "S%d Start function return index %d at term %d isLeader %v", rf.me, index, term, isLeader)
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
	for p := 0; p < len(rf.peers); p++ {
		if p != rf.me {
			// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
			go func(peerId int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				// keep updating this as well
				args.PrevLogIndex = rf.nextIndex[peerId] - 1
				if args.PrevLogIndex <= -1 {
					args.PrevLogTerm = -1
				} else {
					args.PrevLogTerm = rf.log[rf.nextIndex[peerId]-1].ReceivedTerm
				}
				entries := []LogEntry{}
				if rf.nextIndex[peerId] < len(rf.log) {
					entries = rf.log[rf.nextIndex[peerId]:]
				}
				args.Entries = make([]LogEntry, len(entries))
				copy(args.Entries, entries)
				args.LeaderCommit = rf.commitIndex
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(peerId, &args, &reply)
				// If successful: update nextIndex and matchIndex for follower (§5.3)
				if !ok {
					return
				}
				
				
				if reply.Append {
					if reply.Success == true {
						rf.mu.Lock()
						rf.nextIndex[peerId] = lastLogIndex + 1
						rf.matchIndex[peerId] = args.PrevLogIndex + len(entries)
						rf.mu.Unlock()
					} else { // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
						rf.mu.Lock()
						rf.nextIndex[peerId] = max(1, rf.nextIndex[peerId]-1)
						rf.mu.Unlock()
					}
				} else {
					rf.mu.Lock()
					rf.role = Follower
					rf.votedFor = -1
					rf.mu.Unlock()
					return
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
			}(p)
		}
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
		// the election timer expiration
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ResetElectionTimer()
			currentRole := rf.role
			rf.mu.Unlock()
			if currentRole == Leader { // Do not start election on leader
				break
			}
			rf.mu.Lock()
			rf.role = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.mu.Unlock()
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
			rf.mu.Lock()
			Debug(dLog, "S%d is candidate, current term %d", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			// create a background goroutine that will kick off leader election periodically
			// by sending out RequestVote RPCs when it hasn't heard back from another peer for a while
			// issues RequestVote RPCs in parallel to each of the other servers in the cluster.
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				reply := RequestVoteReply{}
				Debug(dLog, "S%d sending out RequestVote", rf.me)
				go func(peerId int) {
					ok := rf.sendRequestVote(peerId, &args, &reply)
					if ok {
						voteResultChan <- reply.VoteGranted
					} else {
						voteResultChan <- false
					}
				}(i)
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
			rf.mu.Lock()
			Debug(dLog, "S%d has %d numVotes in Term %d", rf.me, numVotes, rf.currentTerm)
			rf.mu.Unlock()

			// if state changed during election, ignore the couting
			rf.mu.Lock()
			if rf.role != Candidate {
				Debug(dTimer, "S%d is no longer candidate", rf.me)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			if numVotes >= len(rf.peers)/2+1 {
				rf.mu.Lock()
				Debug(dLeader, "S%d Achieved Majority for T%d (%d), converting to Leader", rf.me, rf.currentTerm, numVotes)
				
				rf.role = Leader
				// rf.ResetElectionTimer()
				// rf.currentLeader = rf.me
				rf.mu.Unlock()
				Debug(dLog, "S%d is leader now!", rf.me)
				rf.BroadcastAppendEntries()
			} else {
				rf.votedFor = -1
			}
			rf.persist()
		// the heartbeat timer expiration
		case <-rf.heartbeatTicker.C:	
			rf.mu.Lock()	
			state := rf.role
			rf.mu.Unlock()
			if state == Leader {
				Debug(dTimer, "S%d Leader broadcast heartbeats", rf.me)
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

	rf.electionTimer = time.NewTimer(time.Duration(rand.Intn(150)+150) * time.Millisecond)
	rf.heartbeatTicker = time.NewTicker(time.Duration(150) * time.Millisecond)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
