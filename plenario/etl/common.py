import tempfile
import requests


class ETLFile(object):
    """
    Encapsulates whether a file has been downloaded temporarily
    or is coming from the local file system.
    If initialized with source_path, it opens file on local filesystem.
    If initialized with source_url, it attempts to download file.

    Implements context manager interface with __enter__ and __exit__.
    """
    def __init__(self, source_path=None, source_url=None):
        if source_path and source_url:
            raise RuntimeError('ETLFile takes exactly one of source_path and source_url. Both were given.')

        if not source_path and not source_url:
            raise RuntimeError('ETLFile takes exactly one of source_path and source_url. Neither were given.')

        self.source_path = source_path
        self.source_url = source_url
        self.is_local = bool(source_path)
        self._handle = None

    def __enter__(self):
        """
        Assigns an open file object to self.file_handle
        """
        if self.is_local:
            self.handle = open(self.source_path, 'r')
        else:
            self._download_temp_file(self.source_url)

        # Return the whole ETLFile so that the `with foo as bar:` syntax looks right.
        return self

    # Users of the class were seeking to 0 all the time after they grabbed the handle.
    # Moved it here so clients are always pointed to 0 when they get handle
    @property
    def handle(self):
        self._handle.seek(0)
        return self._handle

    @handle.setter
    def handle(self, val):
        self._handle = val

    def __exit__(self, exc_type, exc_val, exc_tb):
        # If self.handle is to a file that was already on the file system,
        # .close() acts as we expect.
        # If self.handle is to a TemporaryFile that we downloaded for this purpose,
        # .close() also deletes it from the filesystem.
        self.handle.close()

    def _download_temp_file(self, url):
        """
        Download file to local data directory.
        :param url: url from where file should be downloaded
        :type url: str
        :raises: IOError
        """

        # The file might be big, so stream it in chunks.
        file_stream_request = requests.get(url, stream=True, timeout=5)
        # Raise an exception if we didn't get a 200
        file_stream_request.raise_for_status()

        # Make this temporary file our file handle
        self.handle = tempfile.TemporaryFile()

        # Download and write to disk in 1MB chunks.
        for chunk in file_stream_request.iter_content(chunk_size=1024):
            if chunk:
                self._handle.write(chunk)
                self._handle.flush()
