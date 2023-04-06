from fastapi.testclient import TestClient

from models import (
    GallicaPageContext,
    GallicaRecordFullPageText,
    GallicaRowContext,
    UserResponse,
)
from main import app

client = TestClient(app)


def test_fetch_records_from_gallica_default():
    response = client.get(
        "/api/gallicaRecords", params={"terms": ["test"], "sort": "date"}
    )
    assert response.status_code == 200
    user_response = UserResponse.parse_obj(response.json())
    assert isinstance(user_response.records[0], GallicaPageContext)


def test_fetch_records_from_gallica_row_split():
    response = client.get(
        "/api/gallicaRecords", params={"terms": ["test"], "row_split": True}
    )
    assert response.status_code == 200
    user_response = UserResponse.parse_obj(response.json())
    assert isinstance(user_response.records[0], GallicaRowContext)


def test_fetch_records_from_gallica_include_page_text():
    response = client.get(
        "/api/gallicaRecords", params={"terms": ["test"], "include_page_text": True}
    )
    assert response.status_code == 200
    user_response = UserResponse.parse_obj(response.json())
    assert isinstance(user_response.records[0], GallicaRecordFullPageText)
