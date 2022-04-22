import pytest

from astro.files.locations import location_factory


def test_get_transport_params_for_gcs():  # skipcq: PYL-W0612, PTC-W0065
    """test get_transport_params() method which should return gcs client"""
    path = "gs://bucket/some-file"
    location = location_factory(path)
    credentials = location.get_transport_params()
    assert "google.cloud.storage.client.Client" in str(credentials["client"].__class__)


@pytest.mark.integration
@pytest.mark.parametrize(
    "remote_file",
    [{"name": "google", "count": 2}],
    ids=["google"],
    indirect=True,
)  # skipcq: PY-D0003
def test_remote_object_store_prefix(remote_file):
    """with remote filepath having prefix"""
    _, objects_uris = remote_file
    objects_prefix = objects_uris[0][:-5]
    location = location_factory(objects_prefix)
    assert len(objects_uris) == 2
    assert sorted(location.get_paths()) == sorted(objects_uris)
