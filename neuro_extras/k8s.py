import base64
import json
from pathlib import Path
from typing import Any, Dict

from neuromation import api as neuro_api

from neuro_extras.image_builder import ImageBuilder


async def _create_k8s_secret(name: str) -> Dict[str, Any]:
    async with neuro_api.get() as client:
        payload: Dict[str, Any] = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": name},
            "type": "Opaque",
            "data": {},
        }
        config_path = Path(client.config._path)
        for path in config_path.iterdir():
            if path.is_dir() or path.name in ("db-shm", "db-wal"):
                continue
            payload["data"][path.name] = base64.b64encode(path.read_bytes()).decode()
        return payload


async def _create_k8s_registry_secret(name: str) -> Dict[str, Any]:
    async with neuro_api.get() as client:
        builder = ImageBuilder(client)
        docker_config = await builder.create_docker_config()
        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": name},
            "type": "kubernetes.io/dockerconfigjson",
            "data": {
                ".dockerconfigjson": base64.b64encode(
                    json.dumps(docker_config.to_primitive()).encode()
                ).decode(),
            },
        }
