# Copyright (C) 2025 Bunting Labs, Inc.

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import pytest


@pytest.mark.anyio
async def test_get_available_basemaps(auth_client):
    """Test that the /api/basemaps/available endpoint returns expected basemaps."""
    response = await auth_client.get("/api/basemaps/available")
    assert response.status_code == 200

    data = response.json()
    assert "styles" in data
    assert isinstance(data["styles"], list)
    assert "openstreetmap" in data["styles"]
    assert "openfreemap" in data["styles"]


@pytest.mark.anyio
async def test_create_map_and_update_basemap(auth_client):
    """Test creating a map and updating its basemap setting."""
    # First create a map
    create_payload = {
        "title": "Basemap Test Map",
        "description": "A test map for basemap functionality",
    }
    create_response = await auth_client.post("/api/maps/create", json=create_payload)
    assert create_response.status_code == 200
    map_data = create_response.json()
    map_id = map_data["id"]

    # Update the map's basemap to openfreemap
    update_payload = {"basemap": "openfreemap"}
    update_response = await auth_client.patch(
        f"/api/maps/{map_id}", json=update_payload
    )
    assert update_response.status_code == 200

    update_data = update_response.json()
    assert "message" in update_data
    assert update_data["map_id"] == map_id
    assert update_data["basemap"] == "openfreemap"

    # Get the style.json and verify it uses the openfreemap basemap
    style_response = await auth_client.get(f"/api/maps/{map_id}/style.json")
    assert style_response.status_code == 200

    style_data = style_response.json()
    assert "metadata" in style_data
    assert "current_basemap" in style_data["metadata"]
    assert style_data["metadata"]["current_basemap"] == "openfreemap"
    # OpenFreeMap provides its own style JSON, so just verify it's a valid MapLibre style
    assert "version" in style_data
    assert "sources" in style_data
    assert "layers" in style_data


@pytest.mark.anyio
async def test_create_map_and_update_basemap_to_openstreetmap(auth_client):
    """Test creating a map and updating its basemap to openstreetmap."""
    # First create a map
    create_payload = {
        "title": "OSM Test Map",
        "description": "A test map for OSM basemap",
    }
    create_response = await auth_client.post("/api/maps/create", json=create_payload)
    assert create_response.status_code == 200
    map_data = create_response.json()
    map_id = map_data["id"]

    # Update the map's basemap to openstreetmap
    update_payload = {"basemap": "openstreetmap"}
    update_response = await auth_client.patch(
        f"/api/maps/{map_id}", json=update_payload
    )
    assert update_response.status_code == 200

    update_data = update_response.json()
    assert update_data["basemap"] == "openstreetmap"

    # Get the style.json and verify it uses the openstreetmap basemap
    style_response = await auth_client.get(f"/api/maps/{map_id}/style.json")
    assert style_response.status_code == 200

    style_data = style_response.json()
    assert style_data["metadata"]["current_basemap"] == "openstreetmap"
    assert style_data["name"] == "OpenStreetMap"
    assert "osm" in style_data["sources"]


@pytest.mark.anyio
async def test_map_style_defaults_to_first_available_basemap(auth_client):
    """Test that a new map without a basemap setting uses the first available basemap."""
    # Create a map
    create_payload = {
        "title": "Default Basemap Test",
        "description": "Test default basemap behavior",
    }
    create_response = await auth_client.post("/api/maps/create", json=create_payload)
    assert create_response.status_code == 200
    map_data = create_response.json()
    map_id = map_data["id"]

    # Get the style.json without setting a basemap
    style_response = await auth_client.get(f"/api/maps/{map_id}/style.json")
    assert style_response.status_code == 200

    style_data = style_response.json()
    # Should default to openstreetmap (first available)
    assert (
        style_data["metadata"]["current_basemap"] is None
        or style_data["metadata"]["current_basemap"] == "openstreetmap"
    )
    assert style_data["name"] == "OpenStreetMap"


@pytest.mark.anyio
async def test_update_basemap_with_invalid_name(auth_client):
    """Test updating basemap with an invalid basemap name still works (provider handles it)."""
    # Create a map first
    create_payload = {
        "title": "Invalid Basemap Test",
        "description": "Test invalid basemap handling",
    }
    create_response = await auth_client.post("/api/maps/create", json=create_payload)
    assert create_response.status_code == 200
    map_data = create_response.json()
    map_id = map_data["id"]

    # Update with invalid basemap name
    update_payload = {"basemap": "nonexistent"}
    update_response = await auth_client.patch(
        f"/api/maps/{map_id}", json=update_payload
    )
    assert update_response.status_code == 200

    update_data = update_response.json()
    assert update_data["basemap"] == "nonexistent"

    # Get style should still work (provider handles invalid names)
    style_response = await auth_client.get(f"/api/maps/{map_id}/style.json")
    assert style_response.status_code == 200

    style_data = style_response.json()
    # Invalid basemap should default to openstreetmap
    assert style_data["metadata"]["current_basemap"] == "nonexistent"
    assert style_data["name"] == "OpenStreetMap"  # Provider defaults to OSM


@pytest.mark.anyio
async def test_update_nonexistent_map_basemap(auth_client):
    """Test updating basemap on a nonexistent map returns 404."""
    update_payload = {"basemap": "openfreemap"}
    update_response = await auth_client.patch(
        "/api/maps/nonexistent123", json=update_payload
    )
    assert update_response.status_code == 404


@pytest.mark.anyio
async def test_update_map_empty_payload(auth_client):
    """Test updating map with empty payload returns appropriate message."""
    # Create a map first
    create_payload = {
        "title": "Empty Update Test",
        "description": "Test empty update payload",
    }
    create_response = await auth_client.post("/api/maps/create", json=create_payload)
    assert create_response.status_code == 200
    map_data = create_response.json()
    map_id = map_data["id"]

    # Send empty update
    update_payload = {}
    update_response = await auth_client.patch(
        f"/api/maps/{map_id}", json=update_payload
    )
    assert update_response.status_code == 200

    update_data = update_response.json()
    assert "message" in update_data
    assert "No basemap update provided" in update_data["message"]


@pytest.mark.anyio
async def test_render_basemap_openstreetmap(auth_client):
    """Test rendering OpenStreetMap basemap."""
    response = await auth_client.get(
        "/api/basemaps/render.png",
        params={"basemap": "openstreetmap"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"

    content = response.content
    assert len(content) > 1000
    assert content.startswith(b"\x89PNG")


@pytest.mark.anyio
async def test_render_basemap_openfreemap(auth_client):
    """Test rendering OpenFreeMap basemap."""
    response = await auth_client.get(
        "/api/basemaps/render.png",
        params={"basemap": "openfreemap"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"

    content = response.content
    assert len(content) > 1000
    assert content.startswith(b"\x89PNG")


@pytest.mark.anyio
async def test_render_basemap_invalid_basemap_name(auth_client):
    """Test rendering with invalid basemap name returns 400."""
    response = await auth_client.get(
        "/api/basemaps/render.png",
        params={"basemap": "nonexistent"},
    )
    assert response.status_code == 400
    error_data = response.json()
    assert "Invalid basemap" in error_data["detail"]
    assert "Available options:" in error_data["detail"]
