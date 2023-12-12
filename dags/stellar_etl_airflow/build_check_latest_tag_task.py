import logging

import requests


def get_commit_details(owner, repo, commit_sha):
    url = f"https://api.github.com/repos/{owner}/{repo}/commits/{commit_sha}"
    response = requests.get(url)
    if response.status_code == 200:
        try:
            commit_details = response.json()
            commit_date = commit_details["commit"]["committer"]["date"]
            return commit_date
        except ValueError:
            raise Exception("Error parsing response from GitHub API")
    else:
        raise Exception(
            f"Error fetching commit details from GitHub API: {response.status_code}"
        )


def get_latest_tag_from_github(owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}/tags"
    response = requests.get(url)
    if response.status_code == 200:
        try:
            tags = response.json()
            for tag in tags:
                if "beta" not in tag["name"]:
                    if "alpha" not in tag["name"]:
                        logging.info(f"Tag: {tag['name']}")
                        commit_sha = tag["commit"]["sha"]
                        commit_date = get_commit_details(owner, repo, commit_sha)
                        tag["commit_date"] = commit_date
                        return tag
            return None
        except ValueError:
            raise Exception("Error parsing response from GitHub API")
    else:
        raise Exception(f"Error fetching tags from GitHub API: {response.status_code}")


def is_new_tag(repo, last_known_tag_date):
    repository_owner = "stellar"
    latest_tag = get_latest_tag_from_github(repository_owner, repo)
    if latest_tag:
        logging.info(f"Latest tag date: {latest_tag['commit_date']}")
        if latest_tag["commit_date"] > last_known_tag_date:
            logging.info("New tag found!")
            return True
        else:
            logging.info("No new tags found")
            return "task_aready_up_to_date_task"
