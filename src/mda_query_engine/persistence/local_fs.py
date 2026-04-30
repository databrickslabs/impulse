import os

import boto3

from .filesystem import FileSystem


class LocalFS(FileSystem):
    def __init__(self):
        """
        Initialize the LocalFS object.
        """
        pass

    def read_bytes(self, path: str) -> bytes:
        """
        Read bytes from a local file.

        Parameters
        ----------
        path : str
            Path to the file.

        Returns
        -------
        bytes
            Data read from the file.
        """
        with open(path, "rb") as reader:
            return reader.read()

    def write_bytes(self, path: str, data: bytes) -> bool:
        """
        Write bytes to a local file, creating directories if needed.

        Parameters
        ----------
        path : str
            Path to the file.
        data : bytes
            Data to write.

        Returns
        -------
        bool
            True if write was successful, False otherwise.
        """
        directory = os.path.dirname(path)
        try:  # make sure directory exists
            os.makedirs(directory)
        except:
            pass
        with open(path, "wb") as writer:
            writer.write(data)

    def ls(self, *args, **kwargs):
        """
        List files or directories.

        Parameters
        ----------
        *args
            Positional arguments for listing.
        **kwargs
            Keyword arguments for listing.

        Returns
        -------
        list
            List of files or directories.

        Notes
        -----
        Not implemented.
        """
        pass
