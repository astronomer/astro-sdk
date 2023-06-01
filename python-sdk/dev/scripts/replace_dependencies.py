from __future__ import annotations

import argparse
import fileinput
import pathlib
from re import sub
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from pip._internal.utils.packaging import get_requirement

CWD = pathlib.Path(__file__).parent


def _parse_pinned_pacakge_from_cncf(cncf_url: str) -> str:
    """Parse the pinned version from apache-airflow-providers-cncf-kubernetes Download files page [1]

    As we're not able to test apache-airflow-providers-cncf-kubernetes with pinned version directly,
    we'll need to install from the Source Distribution [2].

    Take input cncf_url=https://pypi.org/project/apache-airflow-providers-cncf-kubernetes/6.2.0rc1/
    as an example, this function goes to Download files page and retreieve Source Distribution URL[2]

    [1] https://pypi.org/project/apache-airflow-providers-cncf-kubernetes/6.2.0rc1/#files
    [2] https://files.pythonhosted.org/packages/08/0f/0cf8e0895995055edc5df9575c5cb32debbf25eb7019bb70dc6d14f36e0d/apache-airflow-providers-cncf-kubernetes-6.2.0rc1.tar.gz

    :param cncf_url: PyPI URL of the apache-airflow-providers-cncf-kubernetes with the version to test
    """
    download_package_url = f"{cncf_url}#files"
    req = requests.get(download_package_url)
    soup = BeautifulSoup(req.text, "html.parser")
    first_file_card_comment = soup.find("div", {"class": "file__card"})
    url = first_file_card_comment.a.get("href")
    return f"apache-airflow-providers-cncf-kubernetes @{url}"


def _parse_pinned_package_from_pypi_url(url: str) -> str:
    """Parse the pinned version from PyPI URL

    Take input url=https://pypi.org/project/apache-airflow-providers-apache-livy/3.5.0rc1/
    as an example, this function extract the path of it and returns
    "apache-airflow-providers-apache-livy==3.5.0rc1"

    :param cncf_url: PyPI URL of the provider to test
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
        _parse_pinned_package_from_pypi_url(url)
        if "apache-airflow-providers-cncf-kubernetes" not in url
        else _parse_pinned_pacakge_from_cncf(url)
        for url in package_urls
    ]
    return pinned_packages


def update_pyproject(rc_provider_packages: list[str]):
    """
    Replaces the given provider packages in the pyproject.toml with the given pinned RC versions.
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
        with fileinput.FileInput("pyproject.toml", inplace=True) as pyproject_file:
            for line in pyproject_file:
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
    args = parser.parse_args()
    issue_url = args.issue_url
    rc_provider_packages_list = []
    if issue_url:
        rc_provider_packages_list = parse_providers_release_testing_gh_issue(issue_url)

    update_pyproject(rc_provider_packages_list)
