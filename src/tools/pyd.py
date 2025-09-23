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
from pydantic import BaseModel, ConfigDict


def _strip_titles(obj: Any) -> Any:
    if isinstance(obj, dict):
        obj.pop("title", None)
        for k in list(obj.keys()):
            obj[k] = _strip_titles(obj[k])
        return obj
    if isinstance(obj, list):
        return [_strip_titles(x) for x in obj]
    return obj


def tool_from(fn, model: Type[BaseModel]) -> Dict[str, Any]:
    schema = model.model_json_schema()

    if isinstance(schema, dict):
        schema.setdefault("type", "object")
        schema["additionalProperties"] = False
        schema = _strip_titles(schema)

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
    model_config = ConfigDict(arbitrary_types_allowed=True)
    user_uuid: str
    conversation_id: int
    map_id: str
    session: Any
