package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cutaret/aetherkv/kv"
	"github.com/cutaret/aetherkv/lsm"
	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/zoo"
)

func main() {
	seed := flag.Uint64("seed", 4, "Master simulation seed to replay")
	nodes := flag.Int("nodes", 4, "Number of cluster nodes")
	bugID := flag.Int("bug", 13, "Bug Zoo ID to enable (0 for clean cluster)")
	htmlOut := flag.String("html", "", "Optional path to write Static HTML Trace Viewer")
	flag.Parse()

	bugs := kv.KVBugConfig{
		UseLSM:  true,
		LSMOpts: lsm.DefaultOptions(),
	}
	switch *bugID {
	case 1:
		bugs.RaftBugs.Bug01Fig8CommitAnyTerm = true
	case 2:
		bugs.RaftBugs.Bug02SkipVotePersist = true
	case 3:
		bugs.RaftBugs.Bug03AckBeforeSync = true
	case 4:
		bugs.RaftBugs.Bug04StaleTruncate = true
	case 5:
		bugs.Bug05BlindLeaderRead = true
	case 6:
		bugs.Bug06LeaseUnderSkew = true
	case 7:
		bugs.Bug07NoClientDedup = true
	case 13:
		bugs.RaftBugs.Bug13EvenQuorum = true
	}

	out := zoo.RunClusterSeed(*seed, *nodes, bugs, nil)
	if !out.Failed {
		fmt.Printf("simrun PASS: seed=%d nodes=%d bug=%d completed with 0 violations.\n", *seed, *nodes, *bugID)
		return
	}

	nodeIDs := make([]string, *nodes)
	for i := 0; i < *nodes; i++ {
		nodeIDs[i] = fmt.Sprintf("n%d", i+1)
	}
	k := sim.NewKernel(*seed, nil)
	initialSched := sim.BuildNemesisSchedule(k.Stream("nemesis"), nodeIDs, 14)
	shrunkSched := sim.ShrinkFaultSchedule(initialSched, func(cand []sim.FaultAction) bool {
		r := zoo.RunClusterSeed(*seed, *nodes, bugs, cand)
		return r.Failed && r.Detector == out.Detector
	})

	fmt.Printf("simrun REPLAY CAUGHT VIOLATION: seed=%d bug=%d detector=%s shrink=%d->%d_faults\n  details: %s\n",
		*seed, *bugID, out.Detector, len(initialSched), len(shrunkSched), out.Details)

	if *htmlOut != "" {
		f, err := os.Create(*htmlOut)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create %s: %v\n", *htmlOut, err)
			os.Exit(1)
		}
		defer f.Close()
		_ = sim.RenderHTMLTraceViewer(f, sim.TraceReport{
			Title:             fmt.Sprintf("Bug Zoo #%d Deterministic Replay", *bugID),
			Seed:              *seed,
			NumNodes:          *nodes,
			Detector:          out.Detector,
			ViolationDetails:  out.Details,
			InitialFaultCount: len(initialSched),
			ShrunkFaultCount:  len(shrunkSched),
			Faults:            shrunkSched,
		})
		fmt.Printf("Wrote interactive HTML trace viewer to %s\n", *htmlOut)
	}
}
