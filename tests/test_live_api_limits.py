from __future__ import annotations

from fastapi.testclient import TestClient

from backend.api import create_app


def test_station_lookup_rejects_more_than_twenty_station_ids(app_config):
    client = TestClient(create_app(app_config))

    response = client.post(
        "/v1/stations/lookup",
        json={"station_ids": [f"station-{index}" for index in range(21)]},
    )

    assert response.status_code == 422
