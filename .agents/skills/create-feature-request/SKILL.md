---
name: create-feature-request
description: Create a GitHub feature request for pypto-serving. Use when the user wants to request a serving API, model integration, executor/runtime, batching, configuration, profiling, or documentation enhancement.
---

# Create Feature Request

Create feature requests that follow `.github/ISSUE_TEMPLATE/feature_request.yml`.

## Target Repository

Default to the current repository:

```bash
target_repo="$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || printf '%s\n' hw-native-sys/pypto-serving)"
```

Use `$target_repo` for all issue search, issue view, and issue creation commands. If repository detection fails, it defaults to `hw-native-sys/pypto-serving`.

## Step 1: Authenticate

```bash
gh auth status
```

If GitHub CLI is not authenticated, tell the user to run `gh auth login` and stop.

## Step 2: Read the Template

Read `.github/ISSUE_TEMPLATE/feature_request.yml` and fill every required field.

Required fields:

- `Summary`
- `Area`
- `Motivation / Use Case`

Optional fields:

- `Proposed API / Behavior`
- `Alternatives Considered`
- `Additional Context`

## Step 3: Classify the Area

Use the template dropdown values exactly:

- `Serving API`
- `Model integration`
- `Executor or runtime`
- `Batching and scheduling`
- `Configuration`
- `Profiling or observability`
- `Documentation`
- `Other`

Infer the area from the user's request when clear. If unclear, ask the user.

## Step 4: Check for Existing Issues

Search open issues in `$target_repo` for duplicates before filing.

```bash
gh issue list --repo "$target_repo" --state open --limit 500 --json number,title,labels \
  --jq '.[] | "\(.number)\t\(.title)\t\(.labels | map(.name) | join(","))"'
```

If a title looks related, inspect up to three candidates:

```bash
gh issue view NUMBER --repo "$target_repo"
```

If an exact duplicate exists, do not create a new issue. Show the existing issue URL and stop.

## Step 5: Format the Issue

Use the template title prefix and label:

- Title: `[Feature] <short summary>`
- Labels: `enhancement`

Format the body as markdown:

```markdown
### Summary

<summary>

### Area

<one dropdown value>

### Motivation / Use Case

<motivation>

### Proposed API / Behavior

<proposed behavior, or "N/A">

### Alternatives Considered

<alternatives, or "N/A">

### Additional Context

<additional context, or "N/A">
```

## Step 6: Preview and Confirm

Show the title, labels, target repository, and full body to the user before creating the issue.

Wait for confirmation or requested edits.

## Step 7: Create the Issue

```bash
gh issue create \
  --repo "$target_repo" \
  --title "[Feature] Short summary" \
  --label "enhancement" \
  --body "$(cat <<'EOF'
### Summary

...
EOF
)"
```

After creation, display the issue URL.

## Checklist

- [ ] GitHub CLI authenticated
- [ ] Feature request template read
- [ ] Required fields filled
- [ ] Existing issues checked for duplicates
- [ ] Title, labels, target repository, and body previewed
- [ ] User confirmed
- [ ] Issue created and URL displayed
