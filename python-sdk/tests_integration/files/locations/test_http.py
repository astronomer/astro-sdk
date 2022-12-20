from __future__ import annotations

import pytest

from astro.files.locations import create_file_location


@pytest.mark.integration
def test_size():
    """Test get_size() of for HTTP file."""
    location = create_file_location(
        "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
    )
    assert location.size > 0
