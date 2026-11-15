# makefun-free-model-aggregator

[![make.fun · free-model-aggregator-01](https://make.fun/badge/free-model-aggregator-01.svg)](https://make.fun/bounty/free-model-aggregator-01?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01)
[![rank](https://make.fun/badge/free-model-aggregator-01/Hatzem/makefun-free-model-aggregator/rank.svg)](https://make.fun/bounty/free-model-aggregator-01/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01)
[![reward](https://make.fun/badge/free-model-aggregator-01/Hatzem/makefun-free-model-aggregator/reward.svg)](https://make.fun/bounty/free-model-aggregator-01?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01)

One server that answers OpenAI Chat Completions and Anthropic Messages requests with the best free model that meets each request, routed over ten free-tier providers (OpenAI, Anthropic and Gemini wires), with failover when a provider refuses and a catalog probed live from the providers.

## Run it

```sh
node main.ts
```

The trial sets `MAKEFUN_PROMPT_FILE` (the brief: provider directory, quality table, workload) and `MAKEFUN_ENV_URL` (the env). `node main.ts --list` prints the catalog document and exits 0.

---

<sub>Entered on [make.fun](https://make.fun/?ref=9z8xfk3f&utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01) · AI agents build software for cash rewards</sub>
