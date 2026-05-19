---
name: create-issue
description: Create a GitHub issue for pypto-serving. Use when the user wants to file a bug report, documentation issue, or general issue in the serving repository.
---

# Create GitHub Issue

Create issues that follow `.github/ISSUE_TEMPLATE/` exactly. Use this skill for bug reports, documentation issues, and general issue filing. For feature requests, prefer `/create-feature-request`.

## Target Repository

Default to the current repository:

```bash
target_repo="$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || printf '%s\n' hw-native-sys/pypto-serving)"
```

Use `$target_repo` for serving issue search, issue view, and issue creation commands. If repository detection fails, it defaults to `hw-native-sys/pypto-serving`.

## Step 1: Authenticate

```bash
gh auth status
```

If GitHub CLI is not authenticated, tell the user to run `gh auth login` and stop.

## Step 2: Classify the Issue

Read `.github/ISSUE_TEMPLATE/` and choose the matching template:

| Template | Use When | Labels |
| --- | --- | --- |
| `bug_report.yml` | Startup failure, generation error, batching bug, runtime error, incorrect output | `bug` |
| `documentation.yml` | Missing, incorrect, or unclear documentation | `documentation` |
| `feature_request.yml` | New capability or behavior improvement | `enhancement` |

If the request is a feature request, switch to `/create-feature-request` unless the user specifically asks to use this general workflow.

## Step 3: Bug Report Data

For bug reports, collect or infer:

- Diagnosis: likely component and reason (`pypto-serving`, `pypto-lib`, `pypto`, `simpler`, or `ptoas`)
- Description and reproduction steps
- Exact command, config, or request payload when available
- Environment table
- Host platform
- Device / platform
- Model and checkpoint path
- Logs or traceback

Use these commands to gather local environment details when useful:

```bash
git rev-parse --short HEAD
git submodule status
uname -s -m
python - <<'PY'
try:
    import torch
    print("torch", torch.__version__)
except Exception as exc:
    print("torch unknown:", exc)
try:
    import torch_npu
    print("torch_npu", torch_npu.__version__)
except Exception as exc:
    print("torch_npu unknown:", exc)
PY
cat /usr/local/Ascend/ascend-toolkit/latest/version.cfg 2>/dev/null || echo "CANN not detected"
```

For `pypto`, `simpler`, and `ptoas` versions, use installed package metadata or local checkout details when available. If a value cannot be detected, write `unknown`.

## Step 4: Reproduce Bug Reports

When the user provides a command or script, try to reproduce the bug before filing.

Use the same command as the user when possible. For device commands, preserve relevant environment variables and `task-submit` options.

If the bug does not reproduce, tell the user and ask whether they still want to file it. If the user says no, stop.

## Step 5: Check for Existing Issues

Search open issues in `$target_repo` first:

```bash
gh issue list --repo "$target_repo" --state open --limit 500 --json number,title,labels \
  --jq '.[] | "\(.number)\t\(.title)\t\(.labels | map(.name) | join(","))"'
```

If the diagnosis clearly points outside serving, also search the relevant repository:

- `pypto-lib` issue: `hw-native-sys/pypto-lib`
- `pypto` issue: `hw-native-sys/pypto`
- `simpler` issue: `hw-native-sys/simpler`

For likely duplicates, inspect up to three candidate issues:

```bash
gh issue view NUMBER --repo OWNER/REPO
```

If an exact duplicate exists, do not create a new issue. Show the existing issue URL and stop.

## Step 6: Format the Issue

Use markdown sections matching the selected template labels.

### Bug Report

Title: `[Bug] <short description>`
Labels: `bug`

~~~markdown
### Diagnosis

<component> - <brief reason>

### Description

<description and reproduction steps>

### Command or Request

```bash
<command, config, or request payload>
```

### Environment

| Component | Version |
|---|---|
| pypto-serving | `<short commit>` |
| pypto-lib | `<short commit or submodule status>` |
| pypto | `<version or commit>` |
| simpler | `<version or commit>` |
| ptoas | `<version>` |
| CANN | `<version>` |
| torch / torch-npu | `<versions>` |

### Host Platform

<Linux (aarch64), Linux (x86_64), macOS (arm64), or Other>

### Device / Platform

<device/platform or N/A>

### Model

<model and checkpoint path or N/A>

### Logs

```text
<logs or traceback>
```

### Additional Context

<additional context or N/A>
~~~

### Documentation Issue

Title: `[Docs] <short description>`
Labels: `documentation`

~~~markdown
### Documentation Location

<document, example, config, or section>

### What's Wrong or Missing?

<issue>

### Suggested Improvement

<suggestion or N/A>

### Additional Context

<additional context or N/A>
~~~

## Step 7: Preview and Confirm

Show the title, labels, target repository, and full body to the user before creating the issue.

Wait for confirmation or requested edits.

## Step 8: Create the Issue

```bash
gh issue create \
  --repo "$target_repo" \
  --title "[<prefix>] Short description" \
  --label "<label>" \
  --body "$(cat <<'EOF'
### Diagnosis

...
EOF
)"
```

After creation, display the issue URL.

## Checklist

- [ ] GitHub CLI authenticated
- [ ] Issue classified
- [ ] Required template fields filled
- [ ] Bug reports reproduced when possible
- [ ] Existing issues checked for duplicates
- [ ] Title, labels, target repository, and body previewed
- [ ] User confirmed
- [ ] Issue created and URL displayed
