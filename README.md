# BigBrain

**BigBrain** is a professional, multi-brain AI agent system featuring a CAN bus–inspired message architecture, specialized brains, and rigorous adversarial verification. Designed for scalability and high-confidence AI operations, BigBrain empowers developers to orchestrate complex tasks efficiently.

## Architecture

BigBrain uses a highly robust architecture separating intent, planning, execution, and verification.

```text
+----------------+      +----------------+      +-------------------+
|                |      |                |      |                   |
|  Front Brain   |----->|  Orchestrator  |----->|  Sub-brain Pool   |
|                |      |  (Event Bus)   |      |  (Python/TS/etc)  |
|                |      |                |      |                   |
+----------------+      +--------+-------+      +-------------------+
                                 |
                        +--------+--------+
                        |                 |
                        v                 v
                 +------------+     +-----------+
                 |            |     |           |
                 | TestWriter |     | Architect |
                 |            |     |           |
                 +------------+     +-----------+
```

### Verification Hierarchy

1. **Deterministic Tools (V4/V1)**: Ground truth validation
2. **LLM Opinions (V3)**: Advisory guidance
3. **Human Intervention (V5)**: Tiebreaker

See `docs/architecture.md` for full specification.

## Core Capabilities

- **Execution Modes**:
  - `SIMPLE`: Quick scripts, no tests.
  - `TESTED`: Self-verified with pytest.
  - `DAG`: Full multi-brain adversarial pipeline.
  - `AUTO`: LLM classification with fallback.
- **TestWriter Brain**: Adversarial test generation isolated from implementation logic.
- **AST-Based Attribution**: 3-tier failing file identification.
- **Diff History**: Previous attempts are tracked in fix prompts.
- **Robust Pipeline**: Skeleton-first code generation, dependency graphs via Kahn's topological sort, and cross-file AST linting.

## User Interface

BigBrain provides a morphable web interface that adapts to the current workspace and focus area.

### Home Workspace
![BigBrain Home Workspace](assets/frontend_home.png)

### Python Workspace
![BigBrain Python Workspace](assets/frontend_python.png)

## Supported Models

BigBrain provides extensive support for cutting-edge models, seamlessly configurable through the admin dashboard and gateway configuration:

- **GPT 5.6 Luna**: Excels at labeling requests and crafting targeted, granular tasks.
- **GPT 5.6 Sol & GPT 6 Astra**: High-performance models for deep reasoning and complex DAG execution.
- **Kimi K3 & Claude**: Advanced coding capabilities and extended context handling.

*Note: The admin dashboard correctly applies model overrides to active sessions in real-time, matching sessions by user ID and applying updates instantly.*

## Getting Started

### Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest tests/ -v
```

## Configuration

Gateway config supports explicit model aliases for smooth transitions:

```json
"agents": {
  "defaults": {
    "models": {
      "gpt-luna": "gpt-5.6-luna",
      "kimi-k3": "kimi-coding/k3"
    }
  }
}
```