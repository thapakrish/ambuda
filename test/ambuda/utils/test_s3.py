"""Unit tests for S3Path.to_asset_url and model asset_url methods."""

from unittest.mock import MagicMock

from ambuda.utils.s3 import S3Path


def test_to_asset_url_strips_assets_prefix():
    s3 = S3Path("bucket", "assets/text-exports/gita.xml")
    assert (
        s3.to_asset_url("https://cdn.example.com")
        == "https://cdn.example.com/text-exports/gita.xml"
    )


def test_to_asset_url_nested_path():
    s3 = S3Path("bucket", "assets/bulk/ambuda-xml.zip")
    assert (
        s3.to_asset_url("https://cdn.example.com")
        == "https://cdn.example.com/bulk/ambuda-xml.zip"
    )


def test_to_asset_url_returns_none_for_non_asset_key():
    s3 = S3Path("bucket", "other/path/file.txt")
    assert s3.to_asset_url("https://cdn.example.com") is None


def test_to_asset_url_no_trailing_slash_on_base_url():
    s3 = S3Path("bucket", "assets/file.txt")
    url = s3.to_asset_url("https://cdn.example.com")
    assert not url.startswith("https://cdn.example.com//")


def test_to_asset_url_from_path():
    s3 = S3Path.from_path("s3://my-bucket/assets/text-exports/gita.xml")
    assert (
        s3.to_asset_url("https://cdn.example.com")
        == "https://cdn.example.com/text-exports/gita.xml"
    )


# -- Page.asset_url --


def test_page_asset_url():
    from ambuda.models.proofing import Page

    page = MagicMock(spec=Page)
    page.uuid = "abc-123"
    page.s3_path.return_value = S3Path("my-bucket", "assets/pages/abc-123.jpg")
    url = Page.asset_url(page, "my-bucket", "https://cdn.example.com")
    assert url == "https://cdn.example.com/pages/abc-123.jpg"


# -- TextExport.asset_url --


def test_text_export_asset_url():
    from ambuda.models.texts import TextExport

    export = MagicMock(spec=TextExport)
    export.s3_path = "s3://my-bucket/assets/text-exports/gita.xml"
    url = TextExport.asset_url(export, "https://cdn.example.com")
    assert url == "https://cdn.example.com/text-exports/gita.xml"


def test_text_export_asset_url_non_asset_key():
    from ambuda.models.texts import TextExport

    export = MagicMock(spec=TextExport)
    export.s3_path = "s3://my-bucket/other/gita.xml"
    assert TextExport.asset_url(export, "https://cdn.example.com") is None


# -- BulkExport.asset_url --


def test_bulk_export_asset_url():
    from ambuda.models.texts import BulkExport

    export = MagicMock(spec=BulkExport)
    export.s3_path = "s3://my-bucket/assets/text-exports/ambuda-xml.zip"
    url = BulkExport.asset_url(export, "https://cdn.example.com")
    assert url == "https://cdn.example.com/text-exports/ambuda-xml.zip"


def test_bulk_export_asset_url_non_asset_key():
    from ambuda.models.texts import BulkExport

    export = MagicMock(spec=BulkExport)
    export.s3_path = "s3://my-bucket/other/ambuda-xml.zip"
    assert BulkExport.asset_url(export, "https://cdn.example.com") is None
