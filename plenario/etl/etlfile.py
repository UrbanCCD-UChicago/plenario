from requests import get


def waitfor(fn):

    result = None
    while not result:
        result = fn()
    return result


class ETLFile:

    def __init__(self, stream):
        self.stream = stream
        self.transforms = []

    def readline(self):
        raise NotImplementedError

    def close(self):
        self.stream.close()

    def hook(self, fn):
        self.transforms.append(fn)

    def read(self, position):
        line = self.readline()
        for transform in self.transforms:
            line = transform(line)
        return line


class ETLFileLocal(ETLFile):

    def __init__(self, path):
        super(ETLFileLocal, self).__init__(open(path, 'rb'))

    def readline(self):
        return self.stream.readline()


class ETLFileRemote(ETLFile):

    def __init__(self, url):
        super(ETLFileRemote, self).__init__(get(url, stream=True).iter_lines())

    def readline(self):
        try:
            return waitfor(lambda: next(self.stream)) + b'\n'
        except StopIteration:
            return b''
