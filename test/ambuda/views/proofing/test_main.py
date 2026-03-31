import io

import pytest

from ambuda.views.proofing import main


@pytest.mark.parametrize(
    "path,expected",
    [
        ("book.pdf", True),
        ("book.djvu", False),
        ("book.epub", False),
    ],
)
def test_is_allowed_document_file(path, expected):
    assert main._is_allowed_document_file(path) == expected


def test_index(client):
    resp = client.get("/proofing/")
    assert resp.status_code == 200
    assert ">Proofing<" in resp.text


def test_complete_guide(client):
    resp = client.get("/proofing/help/complete-guide")
    assert "Complete guide" in resp.text


def test_recent_changes(client):
    resp = client.get("/proofing/recent-changes")
    assert "Recent changes" in resp.text


def test_create_project__unauth(client):
    resp = client.get("/proofing/create-project")
    assert resp.status_code == 302


def test_create_project__auth(rama_client):
    resp = rama_client.get("/proofing/create-project")
    assert resp.status_code == 200


def test_create_project__oversized_pdf(rama_client):
    from unittest.mock import patch
    from tempfile import SpooledTemporaryFile

    limit = 128 * 1024 * 1024

    original_tell = SpooledTemporaryFile.tell

    def fake_tell(self):
        pos = original_tell(self)
        self.seek(0, 2)
        end = original_tell(self)
        self.seek(pos)
        if pos == end and end > 0:
            return limit + 1
        return pos

    fake_pdf = io.BytesIO(b"%PDF-1.4 fake")

    with patch.object(SpooledTemporaryFile, "tell", fake_tell):
        resp = rama_client.post(
            "/proofing/create-project",
            data={
                "pdf_source": "local",
                "local_file": (fake_pdf, "big.pdf"),
                "display_title": "Test Project",
            },
            content_type="multipart/form-data",
        )
    assert resp.status_code == 200
    assert "PDF must be under 128 MB" in resp.text


def test_talk(client):
    resp = client.get("/proofing/talk")
    assert "Talk" in resp.text
