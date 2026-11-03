package sim

import (
	"fmt"
	"html"
	"io"
)

// TraceReport bundles a simulation seed's metadata, fault schedule, and trace events for HTML rendering.
type TraceReport struct {
	Title             string
	Seed              uint64
	NumNodes          int
	Detector          string
	ViolationDetails  string
	InitialFaultCount int
	ShrunkFaultCount  int
	Faults            []FaultAction
	Events            []TraceEvent
}

// RenderHTMLTraceViewer writes a self-contained, zero-dependency interactive HTML trace viewer.
func RenderHTMLTraceViewer(w io.Writer, rep TraceReport) error {
	_, err := fmt.Fprintf(w, `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>AetherKV Trace Viewer — %s (Seed %d)</title>
<style>
  body { background: #0d1117; color: #c9d1d9; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; margin: 24px; }
  h1, h2 { color: #58a6ff; margin-bottom: 8px; }
  .banner { background: #3d161b; border: 1px solid #f85149; padding: 14px; border-radius: 6px; margin-bottom: 20px; }
  .banner strong { color: #ff7b72; }
  .stats { display: flex; gap: 16px; margin-bottom: 20px; }
  .card { background: #161b22; border: 1px solid #30363d; padding: 12px 16px; border-radius: 6px; }
  table { width: 100%%; border-collapse: collapse; background: #161b22; border: 1px solid #30363d; margin-bottom: 24px; font-size: 13px; }
  th, td { padding: 8px 12px; border-bottom: 1px solid #21262d; text-align: left; }
  th { background: #21262d; color: #79c0ff; }
  .badge-fault { background: #6e40c9; color: #fff; padding: 2px 6px; border-radius: 4px; }
  .badge-drop { background: #9e6a03; color: #fff; padding: 2px 6px; border-radius: 4px; }
  .badge-exec { background: #238636; color: #fff; padding: 2px 6px; border-radius: 4px; }
</style>
</head>
<body>
<h1>AetherKV Deterministic Simulation Trace Viewer</h1>
<div class="banner">
  <div><strong>Title:</strong> %s</div>
  <div><strong>Firing Detector:</strong> %s</div>
  <div><strong>Violation Explanation:</strong> %s</div>
</div>
<div class="stats">
  <div class="card"><strong>Master Seed:</strong> %d</div>
  <div class="card"><strong>Cluster Size:</strong> %d Nodes</div>
  <div class="card"><strong>Schedule Shrinking:</strong> %d &rarr; %d Minimal Faults</div>
  <div class="card"><strong>Recorded Events:</strong> %d</div>
</div>
<h2>Shrunk Minimal Nemesis Fault Schedule</h2>
<table>
  <thead><tr><th>Fault ID</th><th>Virtual Time (ms)</th><th>Fault Type</th><th>Target A</th><th>Target B</th></tr></thead>
  <tbody>
`,
		html.EscapeString(rep.Title), rep.Seed,
		html.EscapeString(rep.Title),
		html.EscapeString(rep.Detector),
		html.EscapeString(rep.ViolationDetails),
		rep.Seed, rep.NumNodes, rep.InitialFaultCount, rep.ShrunkFaultCount, len(rep.Events))
	if err != nil {
		return err
	}

	for _, f := range rep.Faults {
		_, _ = fmt.Fprintf(w, "    <tr><td>#%d</td><td>%.3f ms</td><td><span class=\"badge-fault\">%s</span></td><td>%s</td><td>%s</td></tr>\n",
			f.ID, float64(f.TimeNs)/1e6, html.EscapeString(string(f.Type)), html.EscapeString(f.TargetA), html.EscapeString(f.TargetB))
	}

	_, _ = fmt.Fprintf(w, `  </tbody>
</table>
<h2>Step-by-Step Event Timeline</h2>
<table>
  <thead><tr><th>Seq #</th><th>Virtual Time (ms)</th><th>Node</th><th>Event Kind</th><th>Details</th></tr></thead>
  <tbody>
`)

	maxEvents := min(len(rep.Events), 250)
	for i := 0; i < maxEvents; i++ {
		ev := rep.Events[i]
		badge := "badge-exec"
		if len(ev.Kind) >= 8 && ev.Kind[:8] == "net_drop" {
			badge = "badge-drop"
		}
		_, _ = fmt.Fprintf(w, "    <tr><td>%d</td><td>%.3f ms</td><td>%s</td><td><span class=\"%s\">%s</span></td><td>%s</td></tr>\n",
			ev.Seq, float64(ev.TimeNs)/1e6, html.EscapeString(ev.Node), badge, html.EscapeString(ev.Kind), html.EscapeString(ev.Details))
	}

	_, err = fmt.Fprintf(w, `  </tbody>
</table>
</body>
</html>
`)
	return err
}
