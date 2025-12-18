def test_index(client):
    resp = client.get("/proofing/tagging/")
    assert ">Tagging<" in resp.text


def test_text(client):
    resp = client.get("/proofing/tagging/pariksha/")
    assert resp.status_code == 200
