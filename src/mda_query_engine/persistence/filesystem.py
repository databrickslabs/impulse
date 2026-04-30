import abc


class FileSystem(abc.ABC):
    def __init__(self):
        """
        Initialize the FileSystem object.
        """
        pass

    @abc.abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """
        Read bytes from the specified file path.

        Parameters
        ----------
        path : str
            Path to the file.

        Returns
        -------
        bytes
            Data read from the file.
        """
        pass

    @abc.abstractmethod
    def write_bytes(self, path: str, data: bytes) -> bool:
        """
        Write bytes to the specified file path.

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
        pass

    @abc.abstractmethod
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
        """
        pass
