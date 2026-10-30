package raft
import (
"encoding/json"
"slices"
"github.com/cutaret/aetherkv/sim"
)
type Config struct {
ID              string
Peers           []string
ElectionTimeout int
HeartbeatPeriod int
Rand            sim.Rand
Bugs            BugConfig
}
type pendingRead struct {
readID    uint64
commitIdx uint64
acks      []string
}
type Node struct {
id    string
peers []string
role  Role
rand  sim.Rand
bugs  BugConfig
term uint64
vote string
log         []Entry
lastLogTerm uint64
lastLogIdx  uint64
commitIdx   uint64
leaderID string
electionTimeout int
heartbeatPeriod int
ticksSinceElec  int
ticksSinceHB    int
randomizedElec  int
nextIndex    map[string]uint64
matchIndex   map[string]uint64
votes        map[string]bool
pendingReads []pendingRead
ready Ready
}
func NewNode(cfg Config, initialHardState HardState, logEntries []Entry) *Node {
n := &Node{
id:              cfg.ID,
peers:           slices.Clone(cfg.Peers),
role:            RoleFollower,
rand:            cfg.Rand,
bugs:            cfg.Bugs,
log:             slices.Clone(logEntries),
electionTimeout: cfg.ElectionTimeout,
heartbeatPeriod: cfg.HeartbeatPeriod,
nextIndex:       make(map[string]uint64),
matchIndex:      make(map[string]uint64),
votes:           make(map[string]bool),
}
slices.Sort(n.peers)
if len(n.log) > 0 {
last := n.log[len(n.log)-1]
n.lastLogIdx = last.Index
n.lastLogTerm = last.Term
}
n.resetElectionTimeout()
return n
}
func (n *Node) quorumSize() int {
if n.bugs.Bug13EvenQuorum {
return (len(n.peers) + 1) / 2
}
return len(n.peers)/2 + 1
}
func (n *Node) resetElectionTimeout() {
n.ticksSinceElec = 0
n.randomizedElec = n.electionTimeout + n.rand.Intn(n.electionTimeout)
}
func (n *Node) Step(msg sim.Message) Ready {
n.ready = Ready{}
var rpc RPCMessage
if len(msg.Payload) > 0 {
_ = json.Unmarshal(msg.Payload, &rpc)
}
switch rpc.Type {
case MsgVote:
n.handleVote(msg.From, rpc)
case MsgVoteResp:
n.handleVoteResp(msg.From, rpc)
case MsgAppend:
n.handleAppend(msg.From, rpc)
case MsgAppendResp:
n.handleAppendResp(msg.From, rpc)
case MsgHeartbeat:
n.handleHeartbeat(msg.From, rpc)
case MsgHeartbeatResp:
n.handleHeartbeatResp(msg.From, rpc)
}
return n.ready
}
func (n *Node) Tick() Ready {
n.ready = Ready{}
n.ticksSinceElec++
if n.role == RoleLeader {
n.ticksSinceHB++
if n.ticksSinceHB >= n.heartbeatPeriod {
n.ticksSinceHB = 0
n.bcastAppend()
}
} else if n.ticksSinceElec >= n.randomizedElec {
n.startElection()
}
return n.ready
}
func (n *Node) Propose(data []byte) Ready {
n.ready = Ready{}
if n.role != RoleLeader {
return n.ready
}
n.lastLogIdx++
entry := Entry{
Term:  n.term,
Index: n.lastLogIdx,
Data:  slices.Clone(data),
}
n.log = append(n.log, entry)
n.lastLogTerm = entry.Term
n.matchIndex[n.id] = n.lastLogIdx
n.ready.Persist = &PersistReq{
HardState: HardState{Term: n.term, Vote: n.vote},
Entries:   []Entry{entry},
}
if len(n.peers) == 1 {
n.commitIdx = n.lastLogIdx
n.ready.Committed = []Entry{entry}
return n.ready
}
n.bcastAppend()
return n.ready
}
func (n *Node) ReadIndex(readID uint64) Ready {
n.ready = Ready{}
if n.role != RoleLeader {
return n.ready
}
if len(n.peers) == 1 {
n.ready.ReadStates = append(n.ready.ReadStates, ReadState{ReadID: readID, Index: n.commitIdx})
return n.ready
}
n.pendingReads = append(n.pendingReads, pendingRead{
readID:    readID,
commitIdx: n.commitIdx,
acks:      []string{n.id},
})
for _, p := range n.peers {
if p == n.id {
continue
}
n.send(p, RPCMessage{
Type:        MsgHeartbeat,
Term:        n.term,
CommitIndex: n.commitIdx,
ReadID:      readID,
})
}
return n.ready
}
func (n *Node) startElection() {
n.role = RoleCandidate
n.term++
n.vote = n.id
n.resetElectionTimeout()
n.votes = make(map[string]bool)
n.votes[n.id] = true
n.ready.Persist = &PersistReq{
HardState: HardState{Term: n.term, Vote: n.vote},
}
if len(n.votes) >= n.quorumSize() {
n.becomeLeader()
return
}
for _, peer := range n.peers {
if peer == n.id {
continue
}
n.send(peer, RPCMessage{
Type:     MsgVote,
Term:     n.term,
LogIndex: n.lastLogIdx,
LogTerm:  n.lastLogTerm,
})
}
}
func (n *Node) stepDown(newTerm uint64) {
n.term = newTerm
n.role = RoleFollower
n.vote = ""
n.pendingReads = nil
if !n.bugs.Bug02SkipVotePersist {
n.ready.Persist = &PersistReq{HardState: HardState{Term: n.term, Vote: n.vote}}
}
}
func (n *Node) handleVote(from string, rpc RPCMessage) {
if rpc.Term > n.term {
n.stepDown(rpc.Term)
}
canVote := (n.vote == "" || n.vote == from) && rpc.Term == n.term
upToDate := rpc.LogTerm > n.lastLogTerm || (rpc.LogTerm == n.lastLogTerm && rpc.LogIndex >= n.lastLogIdx)
grant := canVote && upToDate
if grant {
n.vote = from
n.resetElectionTimeout()
if !n.bugs.Bug02SkipVotePersist {
n.ready.Persist = &PersistReq{HardState: HardState{Term: n.term, Vote: n.vote}}
}
}
n.send(from, RPCMessage{
Type:   MsgVoteResp,
Term:   n.term,
Reject: !grant,
})
}
func (n *Node) handleVoteResp(from string, rpc RPCMessage) {
if rpc.Term > n.term {
n.stepDown(rpc.Term)
return
}
if n.role != RoleCandidate || rpc.Term != n.term || rpc.Reject {
return
}
n.votes[from] = true
if len(n.votes) >= n.quorumSize() {
n.becomeLeader()
}
}
func (n *Node) becomeLeader() {
n.role = RoleLeader
n.leaderID = n.id
for _, p := range n.peers {
n.nextIndex[p] = n.lastLogIdx + 1
n.matchIndex[p] = 0
}
n.matchIndex[n.id] = n.lastLogIdx
n.bcastAppend()
}
func (n *Node) bcastAppend() {
for _, p := range n.peers {
if p == n.id {
continue
}
prevIdx := n.nextIndex[p] - 1
var prevTerm uint64
if prevIdx > 0 && prevIdx <= uint64(len(n.log)) {
prevTerm = n.log[prevIdx-1].Term
}
var ents []Entry
if n.lastLogIdx >= n.nextIndex[p] && n.nextIndex[p] >= 1 {
ents = slices.Clone(n.log[n.nextIndex[p]-1:])
}
n.send(p, RPCMessage{
Type:        MsgAppend,
Term:        n.term,
LogIndex:    prevIdx,
LogTerm:     prevTerm,
CommitIndex: n.commitIdx,
Entries:     ents,
})
}
}
func (n *Node) handleAppend(from string, rpc RPCMessage) {
if rpc.Term > n.term {
n.stepDown(rpc.Term)
}
if rpc.Term < n.term {
n.send(from, RPCMessage{Type: MsgAppendResp, Term: n.term, Reject: true})
return
}
n.role = RoleFollower
n.leaderID = from
n.resetElectionTimeout()
if rpc.LogIndex > 0 {
if rpc.LogIndex > n.lastLogIdx || n.log[rpc.LogIndex-1].Term != rpc.LogTerm {
conflictTerm := uint64(0)
conflictIndex := rpc.LogIndex
if rpc.LogIndex <= n.lastLogIdx {
conflictTerm = n.log[rpc.LogIndex-1].Term
for i := int(rpc.LogIndex) - 1; i >= 0; i-- {
if n.log[i].Term == conflictTerm {
conflictIndex = n.log[i].Index
} else {
break
}
}
} else {
conflictIndex = n.lastLogIdx + 1
}
n.send(from, RPCMessage{
Type:          MsgAppendResp,
Term:          n.term,
Reject:        true,
ConflictIndex: conflictIndex,
ConflictTerm:  conflictTerm,
})
return
}
}
var persistEntries []Entry
var truncFrom uint64
if n.bugs.Bug04StaleTruncate && len(rpc.Entries) > 0 {
firstIdx := rpc.Entries[0].Index
if firstIdx <= n.lastLogIdx+1 {
truncFrom = firstIdx
n.log = slices.Clone(n.log[:firstIdx-1])
n.log = append(n.log, rpc.Entries...)
persistEntries = slices.Clone(rpc.Entries)
if len(n.log) > 0 {
last := n.log[len(n.log)-1]
n.lastLogIdx = last.Index
n.lastLogTerm = last.Term
} else {
n.lastLogIdx = 0
n.lastLogTerm = 0
}
}
} else {
for i, e := range rpc.Entries {
if e.Index <= n.lastLogIdx {
if n.log[e.Index-1].Term != e.Term {
truncFrom = e.Index
n.log = slices.Clone(n.log[:e.Index-1])
n.log = append(n.log, rpc.Entries[i:]...)
persistEntries = slices.Clone(rpc.Entries[i:])
last := n.log[len(n.log)-1]
n.lastLogIdx = last.Index
n.lastLogTerm = last.Term
break
}
} else {
n.log = append(n.log, e)
persistEntries = append(persistEntries, e)
n.lastLogIdx = e.Index
n.lastLogTerm = e.Term
}
}
}
if len(persistEntries) > 0 || truncFrom > 0 {
n.ready.Persist = &PersistReq{
HardState:    HardState{Term: n.term, Vote: n.vote},
Entries:      persistEntries,
TruncateFrom: truncFrom,
}
}
if rpc.CommitIndex > n.commitIdx {
oldCommit := n.commitIdx
n.commitIdx = min(rpc.CommitIndex, n.lastLogIdx)
if n.commitIdx > oldCommit {
n.ready.Committed = slices.Clone(n.log[oldCommit:n.commitIdx])
}
}
n.send(from, RPCMessage{
Type:     MsgAppendResp,
Term:     n.term,
Reject:   false,
LogIndex: n.lastLogIdx,
})
}
func (n *Node) handleAppendResp(from string, rpc RPCMessage) {
if rpc.Term > n.term {
n.stepDown(rpc.Term)
return
}
if n.role != RoleLeader || rpc.Term != n.term {
return
}
if rpc.Reject {
if rpc.ConflictIndex > 0 {
n.nextIndex[from] = rpc.ConflictIndex
} else if n.nextIndex[from] > 1 {
n.nextIndex[from]--
}
n.bcastAppend()
return
}
if rpc.LogIndex > n.matchIndex[from] {
n.matchIndex[from] = rpc.LogIndex
n.nextIndex[from] = rpc.LogIndex + 1
}
for i := n.lastLogIdx; i > n.commitIdx; i-- {
if !n.bugs.Bug01Fig8CommitAnyTerm && n.log[i-1].Term != n.term {
continue
}
matches := 1
for _, peer := range n.peers {
if peer != n.id && n.matchIndex[peer] >= i {
matches++
}
}
if matches >= n.quorumSize() {
oldCommit := n.commitIdx
n.commitIdx = i
n.ready.Committed = slices.Clone(n.log[oldCommit:n.commitIdx])
n.bcastAppend()
break
}
}
}
func (n *Node) handleHeartbeat(from string, rpc RPCMessage) {
if rpc.Term > n.term {
n.stepDown(rpc.Term)
}
if rpc.Term < n.term {
n.send(from, RPCMessage{Type: MsgHeartbeatResp, Term: n.term, Reject: true, ReadID: rpc.ReadID})
return
}
n.role = RoleFollower
n.leaderID = from
n.resetElectionTimeout()
if rpc.CommitIndex > n.commitIdx && rpc.CommitIndex <= n.lastLogIdx {
oldCommit := n.commitIdx
n.commitIdx = rpc.CommitIndex
if n.commitIdx > oldCommit {
n.ready.Committed = slices.Clone(n.log[oldCommit:n.commitIdx])
}
}
n.send(from, RPCMessage{
Type:   MsgHeartbeatResp,
Term:   n.term,
Reject: false,
ReadID: rpc.ReadID,
})
}
func (n *Node) handleHeartbeatResp(from string, rpc RPCMessage) {
if rpc.Term > n.term {
n.stepDown(rpc.Term)
return
}
if n.role != RoleLeader || rpc.Term != n.term || rpc.Reject {
return
}
var remaining []pendingRead
for _, pr := range n.pendingReads {
if pr.readID == rpc.ReadID {
if !slices.Contains(pr.acks, from) {
pr.acks = append(pr.acks, from)
}
if len(pr.acks) >= n.quorumSize() {
n.ready.ReadStates = append(n.ready.ReadStates, ReadState{
ReadID: pr.readID,
Index:  pr.commitIdx,
})
continue
}
}
remaining = append(remaining, pr)
}
n.pendingReads = remaining
}
func (n *Node) send(to string, rpc RPCMessage) {
payload, _ := json.Marshal(rpc)
n.ready.Send = append(n.ready.Send, sim.Message{
From:    n.id,
To:      to,
Type:    string(rpc.Type),
Term:    rpc.Term,
Payload: payload,
})
}
func (n *Node) ID() string           { return n.id }
func (n *Node) Role() Role           { return n.role }
func (n *Node) Term() uint64         { return n.term }
func (n *Node) Vote() string         { return n.vote }
func (n *Node) CommitIdx() uint64    { return n.commitIdx }
func (n *Node) LogSnapshot() []Entry { return slices.Clone(n.log) }
