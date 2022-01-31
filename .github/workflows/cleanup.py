import asyncio
import datetime as dt
import logging
import os
import re
from typing import Any, Dict

import aiohttp


LIST_CONTAINERS_URL = (
    "https://api.github.com/"
    f"users/neuro-inc/packages/container/{os.environ['GH_REPO_NAME']}/versions"
)
STALE_PERIOD = dt.timedelta(
    days=float(os.environ.get("STALE_DAYS", 0)),
    hours=float(os.environ.get("STALE_HOURS", 0)),
)
VERSION_REGEX = r"\d{1,2}\.\d{1,2}\.\d{0,2}"
logging.basicConfig(level=logging.INFO)
if not STALE_PERIOD:
    raise ValueError(
        f"Stale period is 0 ({STALE_PERIOD}), "
        "set 'STALE_DAYS' and/or 'STALE_HOURS' environment variables."
    )


async def main() -> None:
    got_fails = False
    async with aiohttp.ClientSession(
        headers={"Accept": "application/vnd.github.v3+json"},
        auth=aiohttp.BasicAuth(
            login=os.environ["GH_USERNAME"],
            password=os.environ["GH_PASSWORD"],
        ),
    ) as session:
        async with session.get(LIST_CONTAINERS_URL) as resp:
            existing_imgs = await resp.json()
        for img in existing_imgs:
            if should_delete(img):
                async with session.delete(img["url"]) as resp:
                    if resp.ok:
                        logging.info(f"Deleted {img}")
                    else:
                        logging.warning(
                            f"Failed to delete {img}. \nReason: {resp.reason}"
                        )
            else:
                logging.debug(f"Ignoring {img}")
                got_fails = True
    if got_fails:
        exit(1)
    else:
        exit(0)


def should_delete(img: Dict[str, Any]) -> bool:
    created_at = dt.datetime.strptime(img["created_at"], r"%Y-%m-%dT%H:%M:%SZ")
    verdict = (dt.datetime.now() - created_at) > STALE_PERIOD

    tags = img["metadata"]["container"]["tags"]
    if "latest" in tags:
        verdict = False
    for tag in tags:
        if re.match(VERSION_REGEX, tag):
            verdict = False

    return verdict


if __name__ == "__main__":
    asyncio.run(main())
