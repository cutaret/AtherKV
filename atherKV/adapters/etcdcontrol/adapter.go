package etcdcontrol

import (
	"github.com/cutaret/aetherkv/kv"
	"github.com/cutaret/aetherkv/raft"
)

// ControlConfig returns a strictly specification-compliant configuration
// with all Bug Zoo flags disabled, used to validate zero false positives across campaigns.
func ControlConfig() kv.KVBugConfig {
	return kv.KVBugConfig{
		RaftBugs:             raft.BugConfig{},
		Bug05BlindLeaderRead: false,
		Bug06LeaseUnderSkew:  false,
		Bug07NoClientDedup:   false,
	}
}
