import pytest

from astro.files.locations import location_factory


def describe_is_valid_path():
    """test is_valid_path() method"""

    def with_supported_filepaths():  # skipcq: PTC-W0065, PYL-W0612
        """With supported file paths"""
        filepath = "s3://bucket/some-file"
        location = location_factory(filepath)
        assert location.is_valid_path(filepath) is True


def describe_get_transport_params():
    """test get_transport_params() method"""

    def with_s3():  # skipcq: PYL-W0612, PTC-W0065
        """with S3 filepath"""
        path = "s3://bucket/some-file"
        location = location_factory(path)
        credentials = location.get_transport_params()
        assert "botocore.client.S3" in str(credentials["client"].__class__)


@pytest.mark.integration
@pytest.mark.parametrize(
    "remote_file",
    [{"name": "amazon", "count": 2}],
    ids=["amazon"],
    indirect=True,
)  # skipcq: PY-D0003
def test_remote_object_store_prefix(remote_file):
    """with remote filepath having prefix"""
    _, objects_uris = remote_file
    objects_prefix = objects_uris[0][:-5]
    location = location_factory(objects_prefix)
    assert len(objects_uris) == 2
    assert sorted(location.get_paths()) == sorted(objects_uris)
