import requests
import tempfile

from logging import getLogger
from plenario.database import postgres_engine

logger = getLogger(__name__)


class PlenarioETLError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message


class ETLFile(object):
    """
    Encapsulates whether a file has been downloaded temporarily
    or is coming from the local file system.
    If initialized with source_path, it opens file on local filesystem.
    If initialized with source_url, it attempts to download file.

    Implements context manager interface with __enter__ and __exit__.
    """
    def __init__(self, source_path=None, source_url=None, interpret_as='text'):

        logger.info('Begin.')
        logger.info('source_path: {}'.format(source_path))
        logger.info('source_url: {}'.format(source_url))
        logger.info('interpret_as: {}'.format(interpret_as))
        if source_path and source_url:
            raise RuntimeError('ETLFile takes exactly one of source_path and source_url. Both were given.')

        if not source_path and not source_url:
            raise RuntimeError('ETLFile takes exactly one of source_path and source_url. Neither were given.')

        self.interpret_as = interpret_as
        self.source_path = source_path
        self.source_url = source_url
        self.is_local = bool(source_path)
        self._handle = None
        logger.info('End')

    def __enter__(self):
        """
        Assigns an open file object to self.file_handle
        """
        logger.info('Begin.')
        if self.is_local:
            logger.debug('self.is_local: True')
            file_type = 'rb' if self.interpret_as == 'bytes' else 'r'
            self.handle = open(self.source_path, file_type)
        else:
            logger.debug('self.is_local: False')
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

        logger.info('Begin. (url: {})'.format(url))
        # The file might be big, so stream it in chunks.
        # I'd like to enforce a timeout, but some big datasets
        # take more than a minute to start streaming.
        # Maybe add timeout as a parameter.
        file_stream_request = requests.get(url, stream=True)
        # Raise an exception if we didn't get a 200
        file_stream_request.raise_for_status()

        # Make this temporary file our file handle
        self.handle = tempfile.NamedTemporaryFile()

        # Download and write to disk in 1MB chunks.
        for chunk in file_stream_request.iter_content(chunk_size=1024*1024):
            if chunk:
                self._handle.write(chunk)
                self._handle.flush()
        logger.info('End.')


def add_unique_hash(table_name):
    """
    Adds an md5 hash column of the preexisting columns
    and removes duplicate rows from a table.
    :param table_name: Name of table to add hash to.
    """

    logger.info('Begin (table_name: {})'.format(table_name))
    add_hash = '''
    DROP TABLE IF EXISTS temp;
    CREATE TABLE temp AS
      SELECT DISTINCT *,
             md5(CAST(("{table_name}".*)AS text))
                AS hash FROM "{table_name}";
    DROP TABLE "{table_name}";
    ALTER TABLE temp RENAME TO "{table_name}";
    ALTER TABLE "{table_name}" ADD PRIMARY KEY (hash);
    '''.format(table_name=table_name)

    try:
        postgres_engine.execute(add_hash)
    except Exception as e:
        raise PlenarioETLError(repr(e) +
                               '\n Failed to deduplicate with ' + add_hash)
    logger.info('End.')


def delete_absent_hashes(staging_name, existing_name):

    logger.info('Begin.')
    logger.info('staging_name: {}'.format(staging_name))
    logger.info('existing_name: {}'.format(existing_name))
    del_ = """DELETE FROM "{existing}"
                  WHERE hash IN
                     (SELECT hash FROM "{existing}"
                        EXCEPT
                      SELECT hash FROM  "{staging}");""".\
            format(existing=existing_name, staging=staging_name)

    try:
        postgres_engine.execute(del_)
    except Exception as e:
        raise PlenarioETLError(repr(e) + '\n Failed to execute' + del_)
    logger.info('End.')