import pytest

from astro.files.locations.amazon.s3 import S3Location


@pytest.mark.integration
@pytest.mark.integration
def test_size():
    """Test get_size() of for S3 file."""
    location = S3Location(path="s3://astro-sdk/imdb.csv", conn_id="aws_conn")
    assert location.size > 0
