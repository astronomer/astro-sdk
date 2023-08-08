from __future__ import annotations

import argparse
import fileinput
from re import sub
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from pip._internal.utils.packaging import get_requirement


def _parse_pinned_package_from_pypi_url(url: str) -> str:
    """Parse the pinned version from PyPI URL

    Take input url=https://pypi.org/project/apache-airflow-providers-apache-livy/3.5.0rc1/
    as an example, this function extract the path of it and returns
    "apache-airflow-providers-apache-livy==3.5.0rc1"

    :param url: PyPI URL of the provider to test
    """
    package_name, version = urlparse(url).path.split("/")[2:]
    return f"{package_name}=={version}"


def _parse_pypi_url_from_h2_title(h2_soup: BeautifulSoup) -> str:
    """Parse the h2 title for retrieving package PyPI urls

    :param h2_soup: the BeautifulSoup object that contains the h2 title with PyPI urls
    """
    return h2_soup.a.get("href")


def parse_providers_release_testing_gh_issue(issue_url: str) -> list[str]:
    """Parse the pinned packages from The URL of the github issue that announce provider testing
    (e.g., https://github.com/apache/airflow/issues/31322)

    :param issue_url: The URL of the github issue that announce provider testing
    """
    req = requests.get(issue_url)
    soup = BeautifulSoup(req.text, "html.parser")
    first_comment = soup.find("div", {"class": "comment"})
    h2_titles = first_comment.find_all("h2")
    package_urls = [_parse_pypi_url_from_h2_title(h2_title) for h2_title in h2_titles]
    pinned_packages = [
        _parse_pinned_package_from_pypi_url(url) for url in package_urls
    ]
    return pinned_packages


def update_setup_cfg(rc_provider_packages: list[str]):
    """
    Replaces the given provider packages in the setup.cfg with the given pinned RC versions.
    :param rc_provider_packages: list of RC provider packages to be replaced
    """
    for package in rc_provider_packages:
        requirement = get_requirement(package)
        package_name_to_search = requirement.name

        if requirement.specifier:
            pinned_package = f"{package_name_to_search}{requirement.specifier}"
        elif requirement.url:
            pinned_package = f"{package_name_to_search} @{requirement.url}"
        else:
            raise Exception(
                f"Invalid package {package} provided. It needs to be pinned to a specific version."
            )

        with fileinput.FileInput("pyproject.toml", inplace=True) as setup_file:
            for line in setup_file:
                print(sub(f"{package_name_to_search}.*", pinned_package, line), end="")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--issue-url",
        help=(
            "The URL of the github issue that announce provider testing"
            "(e.g., https://github.com/apache/airflow/issues/31322)"
        ),
    )
    group.add_argument(
        "--rc-provider-packages",
        help=(
            "Comma separated list of provider packages with their pinned versions to test the the RC."
            " e.g. 'apache-airflow-providers-amazon==4.0.0rc1, apache-airflow-providers-google==8.1.0rc2'"
        ),
    )
    args = parser.parse_args()
    issue_url = args.issue_url
    rc_provider_packages = args.rc_provider_packages

    rc_provider_packages_list = []
    if issue_url:
        rc_provider_packages_list = parse_providers_release_testing_gh_issue(issue_url)
    elif rc_provider_packages:
        rc_provider_packages_list = args.rc_provider_packages.split(",")

    update_setup_cfg(rc_provider_packages_list)
