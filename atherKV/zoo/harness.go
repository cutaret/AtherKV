package zoo

import (
	"fmt"

	"github.com/cutaret/aetherkv/adapters/porcupine"
	"github.com/cutaret/aetherkv/kv"
	"github.com/cutaret/aetherkv/raft"
	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/statemachine"
	"github.com/cutaret/aetherkv/vfs"
)

// RunOutcome captures the result of a single seeded cluster simulation.
type RunOutcome struct {
	Seed              uint64
	Failed            bool
	Detector          string
	Details           string
	InitialFaultCount int
	ShrunkFaultCount  int
	VirtualTimeNs     uint64
	History           []porcupine.HistoryOp
}

// RunClusterSeed executes a full simulation for (seed, numNodes, bugCfg) with fault schedule.
func RunClusterSeed(seed uint64, numNodes int, bugs kv.KVBugConfig, customSchedule []sim.FaultAction) RunOutcome {
	k := sim.NewKernel(seed, nil)
	net := sim.NewSimNet(k)

	nodeIDs := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		nodeIDs[i] = fmt.Sprintf("n%d", i+1)
	}

	disks := make(map[string]*vfs.SimFS)
	replicas := make(map[string]*kv.Replica)
	observer := raft.NewObserver(nodeIDs)

	for _, id := range nodeIDs {
		fs := vfs.NewSimFS()
		disks[id] = fs
		rep, err := kv.NewReplica(
			id,
			nodeIDs,
			fs,
			"data_"+id,
			k.NodeClock(id),
			net,
			k.Stream("raft:"+id),
			bugs,
		)
		if err != nil {
			panic(err)
		}
		replicas[id] = rep
	}

	var firstViolation *raft.InvariantViolation
	checkAllInvariants := func() {
		if firstViolation != nil {
			return
		}
		views := make([]raft.NodeStateView, 0, len(nodeIDs))
		for _, id := range nodeIDs {
			views = append(views, replicas[id].StateView())
		}
		if v := observer.Check(views); v != nil {
			firstViolation = v
		}
	}

	for _, id := range nodeIDs {
		nodeID := id
		net.RegisterNode(nodeID, func(msg sim.Message) {
			replicas[nodeID].Step(msg)
			checkAllInvariants()
		})
	}

	// Schedule periodic Raft logical ticks every 10ms across all nodes
	var scheduleTicks func(step int)
	scheduleTicks = func(step int) {
		if step >= 80 || firstViolation != nil {
			return
		}
		for _, id := range nodeIDs {
			nodeID := id
			k.NodeScheduler(nodeID).ScheduleAfter(10_000_000, "raft_tick", func() {
				replicas[nodeID].Tick()
				checkAllInvariants()
			})
		}
		k.ScheduleAfter(10_000_000, "tick_driver", func() {
			scheduleTicks(step + 1)
		})
	}
	scheduleTicks(0)

	// Schedule Nemesis faults
	schedule := customSchedule
	if schedule == nil {
		schedule = sim.BuildNemesisSchedule(k.Stream("nemesis"), nodeIDs, 14)
	}

	for _, act := range schedule {
		action := act
		k.ScheduleAfter(action.TimeNs, string(action.Type), func() {
			switch action.Type {
			case sim.FaultPartitionSym:
				net.Partition(action.TargetA, action.TargetB)
			case sim.FaultPartitionAsym:
				net.AsymmetricPartition(action.TargetA, action.TargetB)
			case sim.FaultLinkLossy:
				cfg := net.GetLink(action.TargetA, action.TargetB)
				cfg.DropProb = 0.35
				cfg.DupProb = 0.25
				cfg.ReorderProb = 0.35
				net.SetLink(action.TargetA, action.TargetB, cfg)
			case sim.FaultNodeCrash:
				replicas[action.TargetA].Crash(disks[action.TargetA], k.Stream("disk:"+action.TargetA))
			case sim.FaultNodeRestart:
				_ = replicas[action.TargetA].Restart()
			case sim.FaultNodePause:
				net.PauseNode(action.TargetA, true)
			case sim.FaultNodeResume:
				net.PauseNode(action.TargetA, false)
			case sim.FaultClockSkew:
				k.NodeClock(action.TargetA).SetSkew(action.SkewPPM*10_000, action.SkewPPM)
			case sim.FaultHealAll:
				net.HealAll(nodeIDs)
				for _, id := range nodeIDs {
					_ = replicas[id].Restart()
				}
			}
			checkAllInvariants()
		})
	}

	// Schedule Client Workload (Put, Get, CAS, Retries)
	workRng := k.Stream("workload:clients")
	var history []porcupine.HistoryOp
	nextOpID := 1
	var clientSeq uint64

	for step := 1; step <= 24; step++ {
		invokeDelayNs := uint64(step) * 25_000_000
		k.ScheduleAfter(invokeDelayNs, fmt.Sprintf("client_step_%d", step), func() {
			if firstViolation != nil {
				return
			}
			// Pick a leader (or any live node during partition to test stale leader reads)
			var target *kv.Replica
			for _, id := range nodeIDs {
				if replicas[id].IsLeader() {
					target = replicas[id]
					break
				}
			}
			if target == nil {
				return
			}

			invokeNs := k.Now()
			opID := nextOpID
			nextOpID++
			clientSeq++
			seq := clientSeq
			clientID := uint64(1 + (opID % 2))
			key := "k1"

			if workRng.Intn(2) == 0 {
				val := fmt.Sprintf("v-%d", opID)
				op := statemachine.Op{
					ClientID: clientID,
					SeqNum:   seq,
					Type:     statemachine.OpPut,
					Key:      []byte(key),
					Value:    []byte(val),
				}
				completed := false
				target.SubmitWrite(op, func(res statemachine.Result, ok bool) {
					if ok && res.Applied {
						completed = true
						history = append(history, porcupine.HistoryOp{
							ID:       opID,
							ClientID: clientID,
							Kind:     porcupine.OpPut,
							Key:      key,
							InputVal: val,
							InvokeNs: invokeNs,
							ReturnNs: k.Now(),
						})
					}
				})
				// Timeout check 30ms later -> record indeterminate if not completed
				k.ScheduleAfter(30_000_000, "write_timeout_check", func() {
					if !completed {
						history = append(history, porcupine.HistoryOp{
							ID:            opID,
							ClientID:      clientID,
							Kind:          porcupine.OpPut,
							Key:           key,
							InputVal:      val,
							InvokeNs:      invokeNs,
							Indeterminate: true,
						})
					}
				})
			} else {
				useLease := bugs.Bug06LeaseUnderSkew
				target.SubmitRead(kv.ClientReadReq{
					ReadID:   uint64(opID),
					Key:      []byte(key),
					UseLease: useLease,
					Callback: func(val []byte, found bool, ok bool) {
						if ok {
							history = append(history, porcupine.HistoryOp{
								ID:        opID,
								ClientID:  clientID,
								Kind:      porcupine.OpGet,
								Key:       key,
								OutputVal: string(val),
								InvokeNs:  invokeNs,
								ReturnNs:  k.Now(),
							})
						}
					},
				})
			}
			checkAllInvariants()
		})
	}

	k.RunUntil(1_000_000_000, 10000)

	if firstViolation != nil {
		return RunOutcome{
			Seed:              seed,
			Failed:            true,
			Detector:          firstViolation.Property,
			Details:           firstViolation.Details,
			InitialFaultCount: len(schedule),
			VirtualTimeNs:     k.Now(),
			History:           history,
		}
	}

	lin := porcupine.CheckLinearizable(history)
	if !lin.Linearizable {
		return RunOutcome{
			Seed:              seed,
			Failed:            true,
			Detector:          "PorcupineLinearizability",
			Details:           lin.Explanation,
			InitialFaultCount: len(schedule),
			VirtualTimeNs:     k.Now(),
			History:           history,
		}
	}

	return RunOutcome{
		Seed:          seed,
		Failed:        false,
		VirtualTimeNs: k.Now(),
		History:       history,
	}
}
