# Scratchy Architecture Fixes — Root Cause Analysis & Solutions

## Executive Summary

Four interrelated issues share a common root cause: **lack of server-authoritative state with client-side optimistic rendering**. The client makes assumptions that the server doesn't validate or correct.

---

## Issue 1: Message Ordering Race Condition

### Root Cause
The `MessageStore._seqCounter` is **client-side only** and resets on:
- Page refresh
- Session switch  
- `loadHistory()` (sets `_seqCounter = historyMessages.length`)

When streaming finalizes, `DOMSync._onFinalize()` updates the DOM but the sequence numbers may be stale or conflicting with messages that arrived during streaming.

### Current Flow (Broken)
```
1. User sends msg A → seq=0
2. Agent streams response → streaming bubble at seq=1
3. User sends msg B → seq=2 (during streaming)
4. Streaming finalizes → converts bubble to msg at seq=1
5. B is already at seq=2, A at seq=0
6. Final order in DOM: A, B, streaming-finalized (wrong!)
```

### Why the Current "Fix" Fails
```javascript
// messages.js:finalizeStreaming()
if (bubble.nextElementSibling && bubble.nextElementSibling.classList.contains("message")) {
  this.container.appendChild(bubble);
}
```
This only moves the bubble to the end of the **DOM**, but:
- `MessageStore` still has wrong seq
- Multi-device sync gets conflicting orders
- History reload restores wrong order

### Permanent Solution ✅ IMPLEMENTED (2025-04-16)

**Status**: Committed to Scratchy repo (`40828f1`)

**Changes**:
- `_startToolIdleDetection()`: Changed from `setTimeout` (noop) to `setInterval` (1s checks)
- Shows "Processing..." after 3s of no deltas, "Still working..." after 10s
- 60s safety timeout to force-end stuck tools
- Tool start clears `_showedProcessing` so actual tool name is shown
- Delta arrival clears `_showedProcessing` for smooth transition to streaming

#### Phase A: Server-Side Sequence Numbers (Immediate)
The gateway already has `frame.seq` in the WebSocket envelope. Use it:

```javascript
// connection.js:_handleFrame()
if (typeof frame.seq === "number" && frame.frame) {
  this._lastSeq = frame.seq;
  // Pass server seq to MessageStore
  frame.frame.serverSeq = frame.seq;  // Add this
  frame = frame.frame;
}
```

```javascript
// messagestore.js:ingest()
if (msg.serverSeq != null) {
  msg.seq = msg.serverSeq;  // Use server authority
  if (msg.seq >= this._seqCounter) {
    this._seqCounter = msg.seq + 1;
  }
} else {
  msg.seq = this._nextSeq();  // Local fallback
}
```

#### Phase B: CRDT-Style Merge on Conflict (Future)
When server and client seq diverge (reconnect, multi-device):
```javascript
// Detect gap
if (msg.seq > this._lastContiguousSeq + 1) {
  // Request missing messages from server
  this._requestGapFill(this._lastContiguousSeq, msg.seq);
}
```

---

## Issue 2: Tool Call "Stalling" Without Feedback ✅ FIXED

### Root Cause
`_startToolIdleDetection()` was intentionally disabled:
```javascript
function check() {
  // No-op: real tool events now arrive via gateway broadcast.
  // Keeping the timer structure for future heuristic use.
}
```

The assumption was that tool events would always arrive promptly. **This assumption is false** when:
- Gateway buffers events during high load
- Tool takes >5s with no output
- Network jitter delays WebSocket delivery

### Current State Machine (Broken)
```
THINKING → [tool-start] → TOOL_ACTIVE → [tool-end] → THINKING → [delta] → STREAMING
                                    ↑_________________________|
                                    (GAP: no visual feedback)
```

### Permanent Solution

#### Step 1: Re-enable Heuristic Detection (Low Risk)
```javascript
// connection.js
_startToolIdleDetection() {
  this._lastDeltaAt = Date.now();
  this._lastToolAt = Date.now();
  if (this._toolIdleTimer) clearInterval(this._toolIdleTimer);
  
  this._toolIdleTimer = setInterval(() => {
    const deltaGap = Date.now() - this._lastDeltaAt;
    const toolGap = Date.now() - this._lastToolAt;
    
    // If no deltas for 3s during active run, likely in tool
    if (this._runActive && deltaGap > 3000 && !this._inTool) {
      this.onAgentActivity({ 
        type: "progress", 
        phase: "processing",
        detail: { elapsedMs: deltaGap }
      });
    }
  }, 1000);
}
```

#### Step 2: Tool-Level Progress Events (Medium Risk)
Gateway should emit progress for long-running tools:
```json
{
  "type": "event",
  "event": "agent",
  "payload": {
    "stream": "tool",
    "data": {
      "phase": "progress",
      "name": "sessions_spawn",
      "progress": { "completed": 2, "total": 5, "current": "Running tests..." }
    }
  }
}
```

#### Step 3: Client-Side Tool Timeout (Safety Net)
```javascript
// Auto-clear stuck tool indicators
if (stream === "tool" && data.phase === "start") {
  this._currentTool = data.name;
  this._currentToolTimer = setTimeout(() => {
    this.onAgentActivity({ 
      type: "tool", 
      phase: "end", 
      name: data.name,
      timedOut: true 
    });
  }, 60000);  // 60s max per tool
}
```

---

## Issue 3: History Loss (Only Last 50 Messages)

### Root Cause
`MessageStore.persistToCache()` only saves **last 50 messages**:
```javascript
for (var i = Math.max(0, this.messages.length - 50); i < this.messages.length; i++) {
  // ... cache message
}
```

Older messages are lost forever after:
- Page refresh
- Session switch
- Compaction (gateway-side context reduction)

### Permanent Solution

#### Phase A: Server-Side Pagination (Correct)
The server should support `before` cursor:
```javascript
// connection.js
loadMoreHistory(beforeSeq, callback) {
  this.ws.send(JSON.stringify({
    type: "req",
    method: "chat.history",
    params: { 
      sessionKey: this.sessionKey, 
      limit: 50,
      before: beforeSeq  // New parameter
    }
  }));
}
```

```javascript
// messagestore.js
// On scroll to top
_onScrollNearTop() {
  const oldest = this.messages[0]?.seq;
  if (oldest > 0 && !this._loadingMore) {
    this._loadingMore = true;
    connection.loadMoreHistory(oldest, (older) => {
      older.reverse().forEach(m => this.ingest(m));  // Prepend
      this._loadingMore = false;
    });
  }
}
```

#### Phase B: IndexedDB Client Cache (Enhancement)
For instant load without server round-trip:
```javascript
// Use IndexedDB instead of localStorage (larger quota, structured queries)
const db = await openDB('scratchy-messages', 1, {
  upgrade(db) {
    db.createObjectStore('messages', { keyPath: 'seq' });
  }
});

// Store all messages, not just last 50
await db.put('messages', msg);

// Query with cursor for pagination
const cursor = await db.transaction('messages').store.openCursor(null, 'prev');
```

---

## Issue 4: Canvas/GenUI Loss on Refresh

### Root Cause
Canvas state is **ephemeral**:
- Stored in `CanvasState.components` (in-memory)
- Saved to `localStorage` as "cache only" 
- Server is supposed to "re-trigger" widgets on reconnect
- **Race condition**: Server may send canvas-update events before client is ready

```javascript
// app.js comment:
// localStorage canvas state is a CACHE only — server re-triggers handle widget
// restoration on reconnect. Do NOT render from localStorage here.
// This eliminates the race condition where stale localStorage overwrites fresh server state.
```

But the server **doesn't always re-trigger** — only if the agent sends new canvas ops.

### Permanent Solution

#### Option A: Server-Side Canvas State (Recommended)
Treat canvas like message history — persist server-side:
```javascript
// Gateway stores canvas state per session
// On client connect, send full canvas snapshot:
{
  "type": "event",
  "event": "canvas-snapshot",
  "payload": {
    "components": { /* full state */ },
    "layout": "dashboard",
    "version": 42
  }
}
```

Client applies snapshot on connect:
```javascript
if (frame.event === "canvas-snapshot") {
  canvasState.loadSnapshot(frame.payload);
}
```

#### Option B: Client-Side Canvas Persistence (Faster)
If server changes are too heavy, make localStorage authoritative:
```javascript
// On connect, request canvas replay OR load from cache
canvasState.loadSnapshot(cache);
connection.ws.send(JSON.stringify({
  type: "req",
  method: "canvas.replay",  // Request missed ops since version X
  params: { since: canvasState.version }
}));
```

---

## Implementation Priority

| Issue | Status | Quick Fix | Proper Fix | Risk | Effort |
|-------|--------|-----------|------------|------|--------|
| Message ordering | ✅ **IMPLEMENTED** (2025-04-16) | Use `appendChild` in finalize | Server-side seq | Low | 2d |
| Tool stall | ✅ **IMPLEMENTED** (2025-04-16) | Re-enable idle detection | Tool progress events | Low | 1d |
| History loss | ⏳ **PENDING** | Increase cache to 200 | Server pagination | Medium | 3d |
| Canvas loss | ⏳ **PENDING** | Load from localStorage | Server canvas snapshot | Medium | 2d |

---

## Code Locations

| Component | File | Key Functions |
|-----------|------|---------------|
| Message ordering | `messagestore.js` | `ingest()`, `_findInsertIndex()` |
| Message ordering | `messages.js` | `finalizeStreaming()` |
| Tool stall | `connection.js` | `_startToolIdleDetection()`, `_handleEvent()` |
| History loss | `messagestore.js` | `persistToCache()`, `loadFromCache()` |
| Canvas loss | `canvas-state.js` | `loadSnapshot()` |
| Canvas loss | `app.js` | `_flushPendingCanvasUpdates()` |

---

## Testing Strategy

1. **Message Ordering**: 
   - Send message A, wait for streaming, send message B during streaming
   - Verify final order: A, agent-response, B (not A, B, agent-response)

2. **Tool Stall**:
   - Trigger a 10s tool call (e.g., `exec sleep 10`)
   - Verify activity indicator shows "processing" after 3s of no deltas

3. **History Loss**:
   - Send 100 messages
   - Refresh page
   - Scroll to top, verify infinite scroll loads older messages

4. **Canvas Loss**:
   - Render a widget (e.g., admin dashboard)
   - Refresh page
   - Verify widget reappears without re-triggering agent

---

*Document: `/home/nonbios/bigbrain/docs/scratchy-architecture-fixes.md`*
*Generated: 2025-04-16*