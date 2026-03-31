def test_index(client):
    resp = client.get("/about/")
    assert ">About</h1>" in resp.text


def test_mission(client):
    resp = client.get("/about/mission")
    assert ">Mission</h1>" in resp.text


def test_values(client):
    resp = client.get("/about/values")
    assert ">Values</h1>" in resp.text


def test_people(client):
    resp = client.get("/about/people/")
    assert "Our core team" in resp.text


def test_people_core_redirects(client):
    resp = client.get("/about/people/core")
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/about/people/")


def test_people_proofing_redirects(client):
    resp = client.get("/about/people/proofing")
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/about/people/")


def test_code_and_data(client):
    resp = client.get("/about/code-and-data")
    assert ">Code and Data</h1>" in resp.text


def test_name(client):
    resp = client.get("/about/our-name")
    assert ">Our Name</h1>" in resp.text


def test_contact(client):
    resp = client.get("/about/contact")
    assert ">Contact</h1>" in resp.text


def test_terms(client):
    resp = client.get("/about/terms")
    assert ">Terms of Use</h1>" in resp.text


def test_privacy(client):
    resp = client.get("/about/privacy-policy")
    assert ">Privacy Policy</h1>" in resp.text
