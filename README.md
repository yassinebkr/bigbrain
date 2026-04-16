# BigBrain v0

Multi-brain AI agent system. CAN bus–inspired message architecture, specialized brains, adversarial verification.

## What's New in v0 (April 2025)

### Model Support: Kimi K2.6
- **Kimi K2.6 Code Preview** now supported as primary model
- 1M token context window (vs K2.5's 256k)
- Improved code reasoning and long-context retention
- Model aliasing: `kimi-k2p6` → `kimi-coding/k2.6-code-preview`

### Admin Dashboard Model Switching (Fixed)
- Fixed `_saveModel()` to correctly resolve active sessions
- Now finds most recently updated session matching user ID
- Supports multiple session key formats (`main:webchat:*`, `agent:main:webchat:*`)
- Real-time model override without restart

### Architecture Enhancements
- **Execution Modes**: SIMPLE | TESTED | DAG | AUTO
  - `SIMPLE`: Quick scripts, no tests
  - `TESTED`: Self-verified with pytest
  - `DAG`: Full multi-brain adversarial pipeline
  - `AUTO`: LLM classification with fallback
- **TestWriter Brain**: Adversarial test generation separated from implementation
- **AST-based attribution**: 3-tier failing file identification
- **Diff history**: Previous attempts tracked in fix prompts

### Pipeline Improvements
- Skeleton-first code generation (headers → stubs → implementation)
- Dependency graph with Kahn's topological sort
- Cross-file AST lint before test execution
- Feedback loop reads stdout (not just stderr)
- 3-layer subprocess defense (knowledge + skeleton + test-rewrite)

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Run tests

```bash
pytest tests/ -v
```

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────┐
│ Front Brain │────→│ Orchestrator│────→│ Sub-brain Pool  │
│  (K2.6)     │     │  (event bus)│     │ (Python/TS/etc) │
└─────────────┘     └─────────────┘     └─────────────────┘
                             │
                    ┌────────┴────────┐
                    ↓                 ↓
              ┌──────────┐      ┌──────────┐
              │TestWriter│      │ Architect│
              │(adversarial)    │(planner) │
              └──────────┘      └──────────┘
```

**Verification Hierarchy (LOCKED)**:
1. Deterministic tools (V4/V1) = ground truth
2. LLM opinions = advisory only (V3)
3. Human = tiebreaker (V5)

See `docs/architecture.md` for full spec.

## Model Configuration

Gateway config supports model aliases:
```json
"agents": {
  "defaults": {
    "models": {
      "kimi-k2p5": "kimi-coding/k2p5",
      "kimi-k2p6": "kimi-coding/k2.6-code-preview"
    }
  }
}
```

## Session Model Override

Admin dashboard now correctly applies model overrides to active sessions:
- Matches sessions by user ID (handles key format variations)
- Sorts by `updatedAt` to find most recent
- Applies override to actual active session (not blind key construction)

---
*Last updated: 2025-04-16*