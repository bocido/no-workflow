import requests
import csv
import time

# ======================
# Config (EXPLICIT)
# ======================

INPUT_FILES = [
    "../top_repos/top_repo_200_300.txt",
    "../top_repos/top_repo_300_400.txt",
]

OUTPUT_FILE = "merge_ci_first_violation.csv"

# For each repo, inspect at most the 100 most recent merged PRs
MAX_MERGED_PRS_PER_REPO = 100


GITHUB_TOKEN = ""

# Request timeout (2 minutes)
REQUEST_TIMEOUT = 120  # seconds

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
}

BASE_URL = "https://api.github.com"


# ======================
# GitHub API helpers
# ======================

def get_merged_prs(owner, repo, max_count):
    """
    Return up to `max_count` most recent merged PRs.
    """
    merged = []
    page = 1

    while len(merged) < max_count:
        url = f"{BASE_URL}/repos/{owner}/{repo}/pulls"
        params = {
            "state": "closed",
            "per_page": 30,
            "page": page,
        }

        r = requests.get(
            url,
            headers=HEADERS,
            params=params,
            timeout=REQUEST_TIMEOUT
        )

        if r.status_code != 200:
            break

        prs = r.json()
        if not prs:
            break

        for pr in prs:
            if pr.get("merged_at"):
                merged.append(pr)
                if len(merged) >= max_count:
                    break

        page += 1
        time.sleep(0.3)

    return merged


def check_commit_checks(owner, repo, sha):
    """
    Inspect check runs for a merge commit.
    """
    url = f"{BASE_URL}/repos/{owner}/{repo}/commits/{sha}/check-runs"

    r = requests.get(
        url,
        headers=HEADERS,
        timeout=REQUEST_TIMEOUT
    )

    if r.status_code != 200:
        return "CHECK_NOT_AVAILABLE"

    runs = r.json().get("check_runs", [])

    if not runs:
        return "NO_CHECK_RUNS"

    for run in runs:
        if run.get("conclusion") != "success":
            return "FAILED_OR_INCOMPLETE_CHECK"

    return "ALL_CHECKS_SUCCESS"


# ======================
# Main logic
# ======================

def main():
    # Open CSV in streaming mode
    f = open(OUTPUT_FILE, "w", newline="")
    writer = csv.writer(f)
    writer.writerow([
        "repo",
        "pr_number",
        "merge_commit_sha",
        "ci_status"
    ])
    f.flush()

    # Build repo list from multiple input files
    repo_list = []
    for input_file in INPUT_FILES:
        with open(input_file, "r") as infile:
            for line in infile:
                line = line.strip()
                if line:
                    repo_list.append(line)

    for repo_full in repo_list:
        print(f"[+] Processing {repo_full}")

        try:
            owner, repo = repo_full.split("/")
        except ValueError:
            writer.writerow([repo_full, "", "", "INVALID_REPO_NAME"])
            f.flush()
            continue

        try:
            merged_prs = get_merged_prs(
                owner,
                repo,
                MAX_MERGED_PRS_PER_REPO
            )
        except requests.exceptions.Timeout:
            writer.writerow([repo_full, "", "", "PR_LIST_TIMEOUT"])
            f.flush()
            continue

        if not merged_prs:
            writer.writerow([repo_full, "", "", "NO_MERGED_PRS_OR_API_ERROR"])
            f.flush()
            continue

        violation_found = False

        for pr in merged_prs:
            pr_number = pr.get("number")
            merge_sha = pr.get("merge_commit_sha")

            if not merge_sha:
                continue

            try:
                ci_status = check_commit_checks(owner, repo, merge_sha)
            except requests.exceptions.Timeout:
                writer.writerow([
                    repo_full,
                    pr_number,
                    merge_sha,
                    "CHECK_RUN_TIMEOUT"
                ])
                f.flush()
                violation_found = True
                break  # move on to next repo

            # Target condition:
            # merged PR without successful CI
            if ci_status in ("NO_CHECK_RUNS", "FAILED_OR_INCOMPLETE_CHECK"):
                writer.writerow([
                    repo_full,
                    pr_number,
                    merge_sha,
                    ci_status
                ])
                f.flush()
                violation_found = True
                print(f"    -> Found violation at PR #{pr_number}")
                break   # move on to next repo

            time.sleep(0.3)

        if not violation_found:
            writer.writerow([
                repo_full,
                "",
                "",
                "NO_VIOLATION_FOUND"
            ])
            f.flush()

    f.close()


if __name__ == "__main__":
    main()
