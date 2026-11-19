from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from uns_kit import AsyncUnsClient, load_background_service_access_declaration


async def read_history(controller_base: str, from_time: str, to_time: str):
    declaration = await asyncio.to_thread(load_background_service_access_declaration, Path(__file__).with_name("package.json"))
    if declaration is None:
        raise RuntimeError("Service access declaration required")
    permissions = declaration["permissions"]
    if permissions["scopes"] != ["read:uns"] or len(permissions["accessRules"]) != 1 or any(character in permissions["accessRules"][0] for character in "+#"):
        raise RuntimeError("This reader requires one exact background history source")
    async with AsyncUnsClient(controller_base) as client:
        return await client.history(permissions["accessRules"][0], from_=from_time, to=to_time, limit=100)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--controller-base", required=True)
    parser.add_argument("--from-time", required=True)
    parser.add_argument("--to-time", required=True)
    options = parser.parse_args()
    result = asyncio.run(read_history(options.controller_base, options.from_time, options.to_time))
    if result is None:
        raise RuntimeError("History endpoint unavailable")
    print(f"Read {sum(len(topic.data) for topic in result.results)} bounded history rows")
