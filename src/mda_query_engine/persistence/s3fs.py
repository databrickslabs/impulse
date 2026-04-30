import logging

import boto3

from .filesystem import FileSystem


class S3FS(FileSystem):
    def __init__(self, bucket: str = None):
        """
        Initialize the S3FS object.

        Parameters
        ----------
        bucket : str, optional
            Name of the S3 bucket.
        """
        self.bucket = bucket
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cache = {}

    def read_bytes(self, path: str) -> bytes:
        """
        Read bytes from an S3 object.

        Parameters
        ----------
        path : str
            Path to the S3 object (can include 's3://' and bucket name).

        Returns
        -------
        bytes
            Data read from the S3 object.
        """
        path = path.replace("s3://", "")
        if path.startswith(self.bucket):
            path = path.replace(self.bucket, "")
        if path.startswith("/"):
            path = path[1:]
        if path in self.cache:
            print("reading data from cache %s" % path)
            return self.cache[path]
        self.logger.info("reading data from %s", path)
        print("reading data from %s" % path)
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket)
        obj = bucket.Object(path)
        result = obj.get()["Body"].read()
        self.logger.info("read %d bytes from %s", len(result), path)
        self.cache[path] = result
        return result

    def write_bytes(self, path: str, data: bytes) -> bool:
        """
        Write bytes to an S3 object.

        Parameters
        ----------
        path : str
            Path to the S3 object (can include 's3://' and bucket name).
        data : bytes
            Data to write.

        Returns
        -------
        bool
            True if write was successful, False otherwise.
        """
        path = path.replace("s3://", "")
        if path.startswith(self.bucket):
            path = path.replace(self.bucket, "")
        if path.startswith("/"):
            path = path[1:]
        self.logger.info("writing %d bytes to %s", len(data), path)
        print("writing %d bytes to %s" % (len(data), path))
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket)
        bucket.put_object(Body=data, Key=path)
        return True

    def ls(self, *args, **kwargs):
        pass
