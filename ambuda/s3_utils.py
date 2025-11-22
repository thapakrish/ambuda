import functools

import boto3


@functools.cache
def _get_client():
    return boto3.client("s3")


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
        self.bucket = bucket
        self.key = key

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
        _get_client().put_object(
            Bucket=self.bucket, Key=self.key, Body=content.encode(encoding)
        )

    def upload_file(self, local_path: str):
        _get_client().upload_file(local_path, self.bucket, self.key)

    def download_file(self, local_path: str):
        _get_client().download_file(local_path, self.bucket, self.key)
