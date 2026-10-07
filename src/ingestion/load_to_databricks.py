from databricks.sdk import WorkspaceClient
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]

VOLUME_PATH = "/Volumes/workspace/default/raw_data"

LOCAL_TO_REMOTE = {
    Path("data/raw/smard"): f"{VOLUME_PATH}/smard",
    Path("data/raw/openmeteo"): f"{VOLUME_PATH}/openmeteo"
}

w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

def upload_folder(local_root: Path, remote_root: str):
    files = list(local_root.rglob("*.json"))
    print(f"{local_root}: {len(files)} files to upload")

    for i, local_path in enumerate(files, start=1):
        relative = local_path.relative_to(local_root)
        remote_path = f"{remote_root}/{relative.as_posix()}"

        with open(local_path, "rb") as f:
            w.files.upload(file_path=remote_path, contents=f, overwrite=True)

        if i % 20 == 0 or i == len(files):
            print(f" {i}/{len(files)} uploaded")

if __name__ == "__main__":
    for local_root, remote_root in LOCAL_TO_REMOTE.items():
        upload_folder(local_root, remote_root)