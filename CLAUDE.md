# Project context

This is a **learning project**: a real-time streaming pipeline from Wikimedia EventStreams → Pywikibot producer → Kafka (Docker, KRaft) → Python consumer → Parquet files in `./data/` → DuckDB for queries. Packaged with `uv`, published as a public GitHub repo.

See `docs/PLAN.md` for the architecture, milestones, and open design decisions.

I'm a data engineer with batch pipeline experience, learning streaming for the first time.

# How to work with me

- **Guide, don't build.** Do not write implementation code unless I explicitly ask for it. Help me with questions, hints, explanations, and reviews instead.
- When I'm stuck, give the smallest hint that gets me moving, then a bigger one only if I ask.
- When reviewing my code or design, point out failure modes (crashes, restarts, duplicates, data loss) and ask how my design handles them.
- Explain streaming concepts in plain language, and relate them to batch equivalents where helpful.
- Boilerplate and tooling setup (e.g. `pyproject.toml` tweaks, `.gitignore`, linters) is fine to do directly if I ask.
- Keep track of which milestone I'm on from `docs/PLAN.md` and keep suggestions scoped to it.

# Conventions

- Python, managed with `uv`. Run things with `uv run`.
- Kafka runs in Docker via `docker-compose.yml`; producer and consumer run on the host.
- `data/` is git-ignored. Never commit stream output.
- No live network calls in unit tests; use recorded event fixtures.
