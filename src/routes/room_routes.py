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

import os
import httpx
from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel
from redis import Redis
from src.dependencies.dag import get_project
from src.database.models import MundiProject

router = APIRouter()

redis = Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    decode_responses=True,
)

DRIFTDB_SERVER_URL = os.environ["DRIFTDB_SERVER_URL"]


class RoomResponse(BaseModel):
    room_id: str


@router.get("/{project_id}/room", response_model=RoomResponse)
async def get_project_room(
    project: MundiProject = Depends(get_project),
):
    redis_key = f"project:{project.id}:room_id"
    room_id = redis.get(redis_key)

    if room_id:
        # Validate the room exists before returning it
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(f"{DRIFTDB_SERVER_URL}/room/{room_id}")
                if response.status_code == 200:
                    return RoomResponse(room_id=room_id)
                else:
                    redis.delete(redis_key)
            except Exception:
                redis.delete(redis_key)

    async with httpx.AsyncClient() as client:
        response = await client.post(f"{DRIFTDB_SERVER_URL}/new")

        if response.status_code != 200:
            print(f"Failed to create room: {response.text}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create room",
            )

        response_data = response.json()
        room_id = response_data.get("room")

    if not room_id:
        print(f"Invalid response from DriftDB: {response_data}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Invalid response from room creation service",
        )

    redis.setex(redis_key, 1800, room_id)  # 30 minutes TTL
    print(f"Created and stored new room {room_id} for project {project.id}")

    return RoomResponse(room_id=room_id)
