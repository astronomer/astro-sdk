"""This script fetches the latest runtime image tag from the provided Quay.io repository URL."""
from __future__ import annotations

import sys

import requests
from semantic_version import Version


def get_latest_tag(repository: str) -> str:
    """Get the latest semantic version tag from a Quay.io repository."""
    url = f"https://quay.io/api/v1/repository/{repository}/tag/"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    tags = data["tags"]
    valid_tags = []
    for tag in tags:
        try:
            if not tag["name"].endswith("-base"):
                continue
            version = Version(tag["name"])
            valid_tags.append(version)
        except ValueError:
            continue
    if valid_tags:
        _latest_tag = max(valid_tags)
        return str(_latest_tag)
    else:
        sys.exit("No valid semantic version tags found.")


if __name__ == "__main__":
    _repository = "astronomer/astro-runtime"
    if len(sys.argv) == 2 and sys.argv[1]:
        _repository = sys.argv[1]
    latest_tag = get_latest_tag(repository=_repository)
    print(latest_tag)
