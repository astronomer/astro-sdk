from astro.files.locations.http import HTTPLocation


class SharePointLocation(HTTPLocation):
    """Handler SharePoint location operations"""

    location_type = FileLocation.SHARE_POINT