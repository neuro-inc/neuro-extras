import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest
from deepdiff import DeepDiff


TEST_ASSETS_ROOT = (Path(__file__).parent.parent / "assets").resolve()
APOLO_EXTRAS_ROOT = Path(__file__).parent.parent.parent / "apolo_extras"
LOGGER = logging.getLogger(__name__)


@pytest.mark.skipif(sys.platform != "darwin", reason="Need sh to test this test.")
def test_auth_merge_script(tmp_path: Path) -> None:
    auths = TEST_ASSETS_ROOT / "registry_auths"
    script = APOLO_EXTRAS_ROOT / "assets" / "merge_docker_auths.sh"
    auth1 = auths / "default-registry.json"
    auth2 = auths / "dockerhub-registry.json"
    merged = auths / "merged.json"
    result_file = tmp_path / "result.json"
    # Check assets
    assert script.exists() and script.is_file()
    assert auth1.exists() and auth1.is_file()
    assert auth2.exists() and auth2.is_file()
    assert merged.exists() and merged.is_file()

    os.environ["NE_REGISTRY_AUTH_INVALID"] = "hey there!"
    os.environ["NE_REGISTRY_AUTH_VALID_STR"] = auth1.read_text().replace("\n", "")
    os.environ["NE_REGISTRY_AUTH_VALID_FILE"] = str(auth2)
    os.environ["NE_RESULT_PATH"] = str(result_file)

    proc = subprocess.Popen(
        ["sh", str(script)], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout_b, stderr_b = proc.communicate(timeout=10)
    stdout, stderr = stdout_b.decode(), stderr_b.decode()
    LOGGER.info(stdout)
    LOGGER.info(stderr)
    assert proc.returncode == 0, f"STDOUT: {stdout}\nSTDERR: {stderr}"
    assert (
        stderr.count("NE_REGISTRY") == 1
    ), "Not only 'NE_REGISTRY_AUTH_INVALID' was asssumed invalid"
    expected = json.loads(merged.read_text())
    actual = json.loads(result_file.read_text())
    diff = DeepDiff(expected, actual, ignore_order=True)

    assert diff == {}, diff
