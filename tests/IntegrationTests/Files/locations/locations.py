import pytest

from astro.files.locations import Location


def describe_get_paths():
    @pytest.mark.integration
    @pytest.mark.parametrize(
        "remote_file",
        [{"name": "google", "count": 2}, {"name": "amazon", "count": 2}],
        ids=["google", "amazon"],
        indirect=True,
    )
    def with_remote_object_store_prefix(remote_file):
        _, objects_uris = remote_file
        objects_prefix = objects_uris[0][:-5]
        location = Location(objects_prefix)
        assert len(objects_uris) == 2
        assert sorted(location.get_paths()) == sorted(objects_uris)
