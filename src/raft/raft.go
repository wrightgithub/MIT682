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

import "sync"
import (
	"labrpc"
	"bytes"
	"encoding/gob"
	"time"
	"math/rand"
	"fmt"
)

// import "bytes"
// import "encoding/gob"
const (
	HEARTBEAT = 50
	ELECTIONBASETIME = 500

	FLLOWER = 0
	LEADER = 1
	CANDIDATE = 2
)

type AppendEntriesArgs  struct {
	Term         int        //领导人的任期号
	LeaderId     int        //领导人的 Id，以便于跟随者重定向请求
	PrevLogIndex int        //新的日志条目紧随之前的索引值
	PrevLogTerm  int        //prevLogIndex 条目的任期号
	Entries      []LogEntry //准备存储的日志条目（表示心跳时为空；一次性发送多个是为了提高效率）
	LeaderCommit int        // 领导人已经提交的日志的索引值
}

type AppendEntriesReply  struct {
	Term    int  //	当前的任期号，用于领导人去更新自己
	Success bool //跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
}


func (rf *Raft)AppendEntries(args AppendEntriesArgs,reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success =false;
	if  args.Term<rf.currentTerm{
		reply.Term =rf.currentTerm
		fmt.Printf("heartbeat is error,args : %d ,me: %d\n",args,rf.currentTerm)
		return
	}

	if args.Term>rf.currentTerm{
		rf.state=FLLOWER
		rf.currentTerm=args.Term
		rf.votedFor = -1
	}
	reply.Term = args.Term
	//fmt.Printf("%d receive heartbeat \n",rf.me)
	rf.heartbeat<-true

	reply.Success =false;
}

// leader发送的
func (rf *Raft)sendAppendEntries( peerNum int,  args AppendEntriesArgs,reply *AppendEntriesReply)bool{
	ok:=rf.peers[peerNum].Call("Raft.AppendEntries",args,reply)
	return ok;
}

func (rf *Raft)broadCastAppendEntries(){
	args:=AppendEntriesArgs{}
	args.Term=rf.currentTerm
	args.LeaderId=rf.me
	args.Entries=append(args.Entries,LogEntry{0,0,0})

	for i, _ := range rf.peers {
		if i==rf.me&&rf.state==LEADER {
			continue
		}
		go func(i int) {
			fmt.Printf("%d broadCastAppendEntries to %d\n",rf.me,i)
			reply:=new(AppendEntriesReply)
			//fmt.Println("args is ",args)
			rf.sendAppendEntries(i,args,reply)
			if reply.Term>rf.currentTerm{
				rf.state=FLLOWER
				rf.currentTerm=reply.Term
			}
		}(i)
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state     int
	voteCount int   //投票计数
	heartbeat chan  bool // 是否有心跳
	electionResult chan bool  //选举结果

	// Persistent state on all servers
	currentTerm int
	votedFor    int  //默认-1为空
	logs        []LogEntry  // 每一个条目包含收到时的任期号和一个用户状态机执行的指令
	// Volatile state on all servers
	commitIndex int   // 已知的最大的已经被提交的日志条目的索引值
	lastApplied int   // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）
	// Volatile state on leaders
	nextIndex  []int // 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	matchIndex []int  // 对于每一个服务器，已经复制给他的日志的最高索引值

}

type  LogEntry struct {
	logIndex int
	LogTerm  int
	Command  interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	isleader= rf.state==LEADER
	term=rf.currentTerm
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	 w := new(bytes.Buffer)
	 e := gob.NewEncoder(w)
	 e.Encode(rf.currentTerm)
	 e.Encode(rf.votedFor)
	 e.Encode(rf.logs)
	 data := w.Bytes()
	 rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	 r := bytes.NewBuffer(data)
	 d := gob.NewDecoder(r)
	 d.Decode(&rf.currentTerm)
	 d.Decode(&rf.votedFor)
	 d.Decode(&rf.logs)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int  //候选人的任期号
	CandidateId int //请求选票的候选人的id
	LastLogIndex int  //候选人的最后日志条目的索引值
	LastLogTerm int //候选人最后日志条目的任期号

}

//
// example RequestVote RPC reply structure.
//

type RequestVoteReply struct {
	// Your data here.
	Term int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool  //候选人赢得了此张选票时为真
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	reply.VoteGranted=false;
	if args.Term < rf.currentTerm{
		reply.Term=rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.state=FLLOWER
		rf.currentTerm=args.Term
		rf.votedFor=-1
	}
	reply.Term = rf.currentTerm

	isUpDate :=false
	if(args.LastLogTerm>rf.getLastLogTerm()){
		isUpDate=true
	}

	if(args.LastLogTerm==rf.getLastLogTerm()&&args.LastLogIndex>=rf.getLastLogIndex()){
		isUpDate=true
	}
	if (rf.votedFor==-1 || rf.votedFor==args.CandidateId)&&isUpDate{
		fmt.Printf("%d vote to %d\n",rf.me,args.CandidateId)
		reply.VoteGranted=true
		return
	}

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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//  由候选人发送
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft)broadCastRequestVote()  {

	args:=RequestVoteArgs{rf.currentTerm,rf.me,rf.getLastLogIndex(),rf.getLastLogTerm()}
	for i,_:=range rf.peers{
		if i==rf.me&&rf.state==CANDIDATE {
			continue
		}
		go func(i int) {
			fmt.Printf("%d sendRequestVote to %d\n",rf.me,i)
			reply:=new(RequestVoteReply)
			ok:=rf.sendRequestVote(i,args,reply)
			if ok {
				if reply.VoteGranted {
					rf.mu.Lock()
					rf.voteCount++
					rf.mu.Unlock()
					//fmt.Println("voteCount is ",rf.voteCount)
					if rf.voteCount>len(rf.peers)/2{
						rf.electionResult<-true
					}
				}
			}
		}(i)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true


	return index, term, isLeader
}


//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}



//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start *goroutines*
// for any long-running work.
//

func (rf *Raft)getLastLogIndex() int{
	return  rf.logs[len(rf.logs)-1].logIndex
}
func (rf *Raft)getLastLogTerm() int{
	return  rf.logs[len(rf.logs)-1].LogTerm
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state=FLLOWER

	// Your initialization code here.
	rf.voteCount=0;
	rf.heartbeat=make(chan bool)
	rf.electionResult=make(chan bool)

	rf.currentTerm=0;
	rf.logs=append(rf.logs,LogEntry{LogTerm:0})
	rf.lastApplied=0;
	rf.votedFor=-1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	
	go func() {
		for  {
			switch rf.state {
			case FLLOWER:
				select {
					case <-rf.heartbeat:
					case <-time.After(time.Duration(rand.Uint32()%300+ELECTIONBASETIME)*time.Millisecond):
						rf.state=CANDIDATE
					}
			case LEADER:
				// todo 发送日志和心跳 AppendEntries
				rf.broadCastAppendEntries()
				time.Sleep(time.Duration(HEARTBEAT)*time.Millisecond)
			case CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor=rf.me
				rf.voteCount=1
				rf.mu.Unlock()
				fmt.Printf("%d become Candidate\n",rf.me)
				// todo 开始选举
				go rf.broadCastRequestVote()
				select {
					case <-time.After(time.Duration(rand.Uint32()%300+ELECTIONBASETIME)*time.Millisecond):
					case <-rf.heartbeat:
						rf.state=FLLOWER
					case <-rf.electionResult:
						rf.mu.Lock()
						rf.state=LEADER
						fmt.Printf("%d become leader\n",rf.me)
						peersNum := len(peers)
						rf.matchIndex = make([]int, peersNum)
						rf.nextIndex = make([]int, peersNum)
						for i:=0;i<peersNum;i++{
							rf.nextIndex[i]=rf.getLastLogIndex()+1;
						}
						rf.mu.Unlock()
					}
			}
		}
		
	}()
	

	return rf
}
