# Scratchy UI/UX Issues — Deep Dive Analysis

## Issue 1: Last Message Not Always Last in UI

### Root Cause
The message ordering bug occurs in the **streaming finalization** phase. When an agent message completes:

1. **Streaming bubble** (`#streaming-message`) receives delta updates in real-time
2. **Final message** arrives via `finalizeStreaming()` which converts the bubble to a permanent message
3. **Race condition**: If the user sends a new message while streaming is active, the new user message gets appended AFTER the streaming bubble
4. **Finalization** then tries to move the agent message to the end, but this doesn't always work correctly

### Current "Fix" (Partial)
In `messages.js:finalizeStreaming()`:
```javascript
// Ensure finalized bubble is the last message (fixes ordering bug)
if (bubble.nextElementSibling && bubble.nextElementSibling.classList.contains("message")) {
  this.container.appendChild(bubble);
}
```

This only works if:
- The streaming bubble still exists when finalization happens
- No other messages were inserted between the bubble and the end

### Failure Cases
1. **Rapid user input**: User sends message B while message A is still streaming → B appears after A in final order
2. **Reconnect during streaming**: WebSocket reconnect creates a new streaming bubble, old one gets orphaned
3. **Multi-device sync**: Remote messages arrive during local streaming → ordering conflict

### Recommended Fix
Track message sequence numbers server-side and reorder client-side on finalization:

```javascript
// In finalizeStreaming, use server-provided sequence instead of DOM order
const serverSeq = frame.payload?.seq || 0;
const messages = Array.from(this.container.querySelectorAll('.message'));
const insertAfter = messages.findLast(m => parseInt(m.dataset.seq || 0) < serverSeq);
if (insertAfter) {
  insertAfter.after(bubble);
}
```

---

## Issue 2: Tool Calls Stall Without Visual Feedback

### Root Cause
The "stalling" is actually the **Tool Idle Detection** being disabled:

```javascript
// connection.js:_startToolIdleDetection()
function check() {
  // No-op: real tool events now arrive via gateway broadcast.
  // Keeping the timer structure for future heuristic use.
}
```

This was intentionally disabled because "real tool events now arrive via gateway broadcast" — but if those events don't arrive or are delayed, **the user sees nothing**.

### Current Flow
1. User sends message
2. `onAgentActivity({ type: "thinking", phase: "start" })` → shows "💭 Thinking..."
3. Tool starts: `stream === "tool" && data.phase === "start"` → shows tool name
4. **Gap**: Between tool start and delta streaming, nothing updates
5. Tool ends: `stream === "tool" && data.phase === "end"` → still shows "thinking"
6. Deltas arrive: `stream === "assistant"` → clears thinking, shows streaming

### Failure Cases
1. **Long-running tools** (>5s): User sees "Reading file" then nothing for 10s+ → feels like stall
2. **Tool failure**: Error events may not arrive → indicator hangs indefinitely
3. **Gateway lag**: WebSocket buffering delays tool events → no visual feedback
4. **Multi-tool chains**: Tool A → Tool B → Tool C, user only sees last tool name

### Current Timeouts (Band-Aids)
- `_runStartTimer`: 15s → clears indicator if no activity after run starts
- `_runEndTimer`: 5s after last delta → clears indicator
- `_stalenessTimer`: 10s no events → health check → reconnect

These don't solve the UX problem — they just prevent infinite hangs.

### Recommended Fixes

#### Option A: Bring Back Tool Idle Detection (Heuristic)
```javascript
_startToolIdleDetection() {
  this._lastDeltaAt = Date.now();
  const check = () => {
    const gap = Date.now() - this._lastDeltaAt;
    if (gap > 3000 && !this._showedToolIdle) {
      // No text for 3s = likely in tool call
      this.onAgentActivity({ 
        type: "tool", 
        phase: "idle", 
        detail: { message: "Processing..." }
      });
      this._showedToolIdle = true;
    }
  };
  this._toolIdleTimer = setInterval(check, 1000);
}
```

#### Option B: Progress Indicators for Known Long Tools
```javascript
const toolProgress = {
  "sessions_spawn": { stages: ["spawning", "running", "finalizing"], estimatedMs: 30000 },
  "browser": { stages: ["navigating", "interacting", "extracting"], estimatedMs: 15000 },
  "exec": { stages: ["starting", "executing", "completing"], estimatedMs: 10000 }
};
```

#### Option C: Heartbeat Dots
Show animated dots after tool name stops changing:
```
📄 Reading file...
📄 Reading file..
📄 Reading file.
(repeating)
```

---

## Related Issues

### Tool Events Not Showing Names
In `connection.js:_extractToolDetail()`:
```javascript
case "sessions_spawn":
  return d.task || "";  // Often empty! Shows blank after colon
```

When `detail.args.task` is undefined, the activity shows:
```
📄 Reading file: /path/to/file  ✓
🤖 Spawning sub-agent:            ✓  (blank!)
```

### Missing Tool End Events
If a tool crashes or the gateway restarts, the `phase: "end"` event may never arrive. The activity indicator stays stuck showing the tool name.

**Fix**: Add tool timeout in `_handleEvent`:
```javascript
if (stream === "tool" && data.phase === "start") {
  this._currentTool = data.name;
  this._currentToolTimer = setTimeout(() => {
    // Force end after 60s no matter what
    this.onAgentActivity({ type: "tool", name: data.name, phase: "end", timedOut: true });
  }, 60000);
}
```

---

## Summary

| Issue | Severity | Fix Complexity | Recommended Action |
|-------|----------|----------------|-------------------|
| Message ordering | Medium | Medium | Add server-side sequence numbers |
| Tool stall UX | High | Low | Re-enable idle detection heuristic |
| Blank tool details | Low | Low | Add fallback text in `_extractToolDetail` |
| Missing tool end | Medium | Low | Add client-side tool timeout |

---

*File: `/home/nonbios/bigbrain/docs/scratchy-ui-issues.md`*
*Generated: 2025-04-16*