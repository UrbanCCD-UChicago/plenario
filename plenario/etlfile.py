from requests import get


def waitfor(fn):

    result = None
    while not result:
        result = fn()
    return result


class ETLFile:

    def __init__(self, stream):
        self.stream = stream

    def close(self):
        self.stream.close()

    def read(self, position):
        raise NotImplementedError


class ETLFileLocal(ETLFile):

    def __init__(self, path):
        super(ETLFileLocal, self).__init__(open(path, 'rb'))

    def read(self, position):
        return self.stream.read(position)


class ETLFileRemote(ETLFile):

    def __init__(self, url):
        stream = get(url, stream=True).iter_content(chunk_size=8192)
        super(ETLFileRemote, self).__init__(stream)

    def read(self, position):
        try:
            return waitfor(lambda: next(self.stream))
        except StopIteration:
            return b''
