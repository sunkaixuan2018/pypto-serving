# Codex Agent Configuration

Repository-local skills are stored canonically in `.agents/skills/`.

The `.codex/skills` path is a symlink to `../.agents/skills`, so Codex-compatible
tooling reads the same skill definitions without maintaining duplicate copies.

Add or update skills under `.agents/skills/`.
