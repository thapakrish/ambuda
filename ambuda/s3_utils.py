import functools
import os
from pathlib import Path

import boto3


@functools.cache
def _get_client():
    client = boto3.client("s3")
    if os.environ["FLASK_ENV"] != "production":
        return LocalFSBotoClient()
    else:
        return client


def _log(msg):
    print(msg)


class LocalFSBotoClient:
    """A development-only client that mocks out all S3 requests using the local filesystem."""

    def __init__(self, base_path: Path | str | None = None):
        if base_path is None:
            base_path = Path(__file__).parent.parent / "data" / "s3_local"
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        _log(f"Using local s3 at base path {base_path}")

    def _get_local_path(self, bucket: str, key: str) -> Path:
        return self.base_path / bucket / key

    def head_object(self, Bucket: str, Key: str, **kwargs):
        local_path = self._get_local_path(Bucket, Key)
        if not local_path.exists():
            raise Exception(f"Object not found: {Bucket}/{Key}")
        return {"ContentLength": local_path.stat().st_size}

    def get_object(self, Bucket: str, Key: str, **kwargs):
        local_path = self._get_local_path(Bucket, Key)
        if not local_path.exists():
            raise Exception(f"Object not found: {Bucket}/{Key}")

        class Body:
            def __init__(self, path):
                self.path = path

            def read(self):
                return self.path.read_bytes()

        return {"Body": Body(local_path)}

    def put_object(self, Bucket: str, Key: str, Body: bytes, **kwargs):
        local_path = self._get_local_path(Bucket, Key)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(Body)
        _log(f"put_object to {local_path}")

        return {"ETag": "mock-etag"}

    def upload_file(self, Filename: str | Path, Bucket: str, Key: str, **kwargs):
        local_path = self._get_local_path(Bucket, Key)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        source = Path(Filename)
        if not source.exists():
            raise Exception(f"Source file not found: {Filename}")
        local_path.write_bytes(source.read_bytes())
        _log(f"upload_file to {local_path}")

    def download_file(self, Bucket: str, Key: str, Filename: str | Path, **kwargs):
        local_path = self._get_local_path(Bucket, Key)
        if not local_path.exists():
            raise Exception(f"Object not found: {Bucket}/{Key}")
        dest = Path(Filename)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(local_path.read_bytes())
        _log(f"download_file to {local_path}")

    def delete_object(self, Bucket: str, Key: str, **kwargs):
        local_path = self._get_local_path(Bucket, Key)
        if local_path.exists():
            local_path.unlink()
        return {}


class S3Path:
    """A simple utiilty for working with S3 paths."""

    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key

    @staticmethod
    def from_path(path: str):
        _prefix = "s3://"
        assert path.startswith(_prefix)
        path = path.removeprefix(_prefix)

        bucket, _, key = path.partition("/")
        return S3Path(bucket, key)

    def __str__(self):
        return self.path

    def __repr__(self):
        return f'S3Path("{self.path}")'

    @property
    def path(self):
        return f"s3://{self.bucket}/{self.key}"

    def exists(self) -> bool:
        try:
            _ = _get_client().head_object(Bucket=self.bucket, Key=self.key)
            return True
        except:
            return False

    def read_text(self, encoding="utf-8") -> str:
        return self.read_bytes().decode(encoding)

    def read_bytes(self) -> bytes:
        obj = _get_client().get_object(Bucket=self.bucket, Key=self.key)
        return obj["Body"].read()

    def write_text(self, content: str, encoding="utf-8"):
        self.write_bytes(content.encode(encoding))

    def write_bytes(self, content: bytes):
        _get_client().put_object(Bucket=self.bucket, Key=self.key, Body=content)

    def upload_file(self, local_path: str | Path):
        _get_client().upload_file(local_path, self.bucket, self.key)

    def download_file(self, local_path: str | Path):
        _get_client().download_file(self.bucket, self.key, local_path)

    def delete(self):
        _get_client().delete_object(Bucket=self.bucket, Key=self.key)

    def _debug_local_path(self) -> Path | None:
        """(Debug only) Get the local path corresponding to this S3 path.

        For production paths, this method always returns None.
        """
        client = _get_client()
        if not isinstance(client, LocalFSBotoClient):
            return None

        return client._get_local_path(self.bucket, self.key)
