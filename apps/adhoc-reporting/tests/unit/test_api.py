from fastapi.testclient import TestClient

from app import app

client = TestClient(app)


def test_health():
    response = client.get("/api/meta/health")
    assert response.status_code == 200
    assert response.json()["impulse_version"]


def test_user_endpoints_require_forwarded_identity():
    assert client.get("/api/reports").status_code == 401
    assert client.get("/api/data/catalogs").status_code == 401
