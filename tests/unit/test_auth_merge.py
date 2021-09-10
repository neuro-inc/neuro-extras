import json
import os
import subprocess
from pathlib import Path

from deepdiff import DeepDiff


ASSETS_ROOT = (Path(__file__).parent.parent / "assets").resolve()


# @pytest.mark.skipif(
#     sys.platform != "linux",
#     reason="Need shell to test this scrip."
# )
def test_auth_merge_script(tmp_file: Path) -> None:
    auths = ASSETS_ROOT / "registry_auths"
    script = Path(__file__).parent.parent.parent / "assets" / "merge_docker_auths.sh"
    auth1 = auths / "default-registry.json"
    auth2 = auths / "dockerhub-registry.json"
    merged = auths / "merged.json"
    # Check assets
    assert script.exists() and script.is_file()
    assert auth1.exists() and auth1.is_file()
    assert auth2.exists() and auth2.is_file()
    assert merged.exists() and merged.is_file()

    os.environ["NE_REGISTRY_AUTH_INVALID"] = "hey there!"
    os.environ["NE_REGISTRY_AUTH_VALID_STR"] = auth1.read_text().replace("\n", "")
    os.environ["NE_REGISTRY_AUTH_VALID_FILE"] = str(auth2)
    os.environ["NE_RESULT_PATH"] = str(tmp_file)

    proc = subprocess.Popen(
        ["sh", str(script)], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout_b, stderr_b = proc.communicate(timeout=10)
    stdout, stderr = stdout_b.decode(), stderr_b.decode()
    print(stdout)
    print(stderr)
    assert proc.returncode == 0, f"STDOUT: {stdout}\nSTDERR: {stderr}"
    assert (
        stderr.count("NE_REGISTRY") == 1
    ), "Not only 'NE_REGISTRY_AUTH_INVALID' was asssumed invalid"
    expected = json.loads(merged.read_text())
    actual = json.loads(tmp_file.read_text())
    diff = DeepDiff(expected, actual, ignore_order=True)

    assert diff == {}, diff
