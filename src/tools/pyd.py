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

from typing import Any, Dict, Type
from pydantic import BaseModel


def tool_from(fn, model: Type[BaseModel]) -> Dict[str, Any]:
    schema = model.model_json_schema()

    if isinstance(schema, dict):
        schema.setdefault("type", "object")
        schema["additionalProperties"] = False

    return {
        "type": "function",
        "function": {
            "name": fn.__name__,
            "description": (fn.__doc__ or "").strip(),
            "strict": True,
            "parameters": schema,
        },
    }


class MundiToolCallMetaArgs(BaseModel):
    user_uuid: str
    conversation_id: int
    map_id: str
