Output docs/assets/demo.gif
Set Shell "bash"
Set FontSize 16
Set Width 1100
Set Height 520
Set Padding 24
Set TypingSpeed 35ms
Set Theme "GruvboxDark"
Set WindowBar Colorful
# Pre-warm Go build cache off-camera so commands run crisply on screen
Hide
Type "export PS1='$ '"
Enter
Type "go run ./cmd/detlint statemachine raft lsm kv >/dev/null 2>&1 && go run ./cmd/simrun -seed 4 -nodes 4 -bug 13 >/dev/null 2>&1 && go test ./sim -run TestS0_Sanity1 >/dev/null 2>&1"
Enter
Wait+Screen@90s /\$/
Ctrl+L
Show
Sleep 400ms
Type "# 1. Static AST/type determinism gate (forbids goroutines, time.Now, rand, maps)"
Enter
Type "go run ./cmd/detlint statemachine raft lsm kv"
Enter
Wait+Screen@20s /PASSED/
Sleep 1500ms
Type "# 2. Replay Bug Zoo #13 (4-node split-brain) & delta-shrink fault schedule"
Enter
Type "go run ./cmd/simrun -seed 4 -nodes 4 -bug 13 -html docs/trace_zoo13.html"
Enter
Wait+Screen@20s /Wrote interactive HTML trace viewer/
Sleep 2500ms
Type "# 3. Verify 100% SHA-256 trace determinism across 20,000 simulated runs"
Enter
Type "go test -v ./sim -run TestS0_Sanity1 -count=1"
Enter
Wait+Screen@30s /PASS/
Sleep 3500ms
