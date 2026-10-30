package raft
import "github.com/cutaret/aetherkv/sim"
type Role uint8
const (
RoleFollower Role = iota + 1
RolePreCandidate
RoleCandidate
RoleLeader
)
type MessageType string
const (
MsgVote          MessageType = "MsgVote"
MsgVoteResp      MessageType = "MsgVoteResp"
MsgPreVote       MessageType = "MsgPreVote"
MsgPreVoteResp   MessageType = "MsgPreVoteResp"
MsgAppend        MessageType = "MsgAppend"
MsgAppendResp    MessageType = "MsgAppendResp"
MsgHeartbeat     MessageType = "MsgHeartbeat"
MsgHeartbeatResp MessageType = "MsgHeartbeatResp"
MsgReadIndex     MessageType = "MsgReadIndex"
MsgReadIndexResp MessageType = "MsgReadIndexResp"
)
type Entry struct {
Term  uint64 `json:"term"`
Index uint64 `json:"index"`
Data  []byte `json:"data"`
}
type HardState struct {
Term uint64 `json:"term"`
Vote string `json:"vote"`
}
type PersistReq struct {
HardState    HardState
Entries      []Entry
TruncateFrom uint64
}
type ReadState struct {
ReadID uint64
Index  uint64
}
type Ready struct {
Persist    *PersistReq
Send       []sim.Message
Committed  []Entry
ReadStates []ReadState
}
type RPCMessage struct {
Type          MessageType `json:"type"`
Term          uint64      `json:"term"`
LogTerm       uint64      `json:"log_term"`
LogIndex      uint64      `json:"log_index"`
CommitIndex   uint64      `json:"commit_index"`
Entries       []Entry     `json:"entries,omitempty"`
Reject        bool        `json:"reject,omitempty"`
ConflictIndex uint64      `json:"conflict_index,omitempty"`
ConflictTerm  uint64      `json:"conflict_term,omitempty"`
ReadID        uint64      `json:"read_id,omitempty"`
}
type BugConfig struct {
Bug01Fig8CommitAnyTerm bool
Bug02SkipVotePersist   bool
Bug03AckBeforeSync     bool
Bug04StaleTruncate     bool
Bug13EvenQuorum        bool
}
