''' Decrypt all CMS files - extracted files are placed in the working directory

Logging is a tested feature:

    >>> from sys import stdout
    >>> logging.basicConfig(level=logging.INFO, stream=stdout)

    >>> enc_path = MockPath.make_ext('/over/there')
    >>> main(enc_path)
    INFO:decrypt_all:Skipped due to filter: f1
    INFO:decrypt_all:mock_chmod: /over/there/res000050354req_f2, 448
    INFO:decrypt_all:Decrypting /over/there/res000050354req_f2
    INFO:decrypt_all:MockPopen::communicate sekret
    <BLANKLINE>
    INFO:decrypt_all:Decrypted 1 files

'''
import stat
from StringIO import StringIO
from itertools import tee
from subprocess import PIPE

import logging  # Exception to OCAP

log = logging.getLogger(__name__)


def main(arg_path,
         filter_prefix='res000050354req',
         decrypt_args=' --overwrite --verbose'):
    '''
    Recursively search for encrypted files matching the filter_prefix pattern.

    @param arg_path: Path to where the delivered HD was copied.
                     (see mix_path_utils)
    '''
    enc_path = arg_path()

    decrypt_count = 0
    for root, dirs, files in walk(enc_path):
        for f in files:
            full_path = f.resolve()
            if full_path.is_file():
                if f.name.startswith(filter_prefix):
                    f.chmod(stat.S_IXUSR | stat.S_IRUSR | stat.S_IWUSR)
                    log.info('Decrypting %s' % full_path)
                    ret = f.decrypt(decrypt_args)
                    if ret:
                        raise RuntimeError('Return %d from "%s"' %
                                           (ret, full_path))
                    decrypt_count += 1
                else:
                    log.info('Skipped due to filter: %s' % f.name)
    log.info('Decrypted %d files' % decrypt_count)


def mix_path_utils(base, popen, password, chmod):
    class PathRestrict(base):

        def pathjoin(self, other):
            there = base(str(self)) / other
            if not there.startswith(str(self)):
                raise IOError('%s not under %s' % (self, there))

    class PathExt(PathRestrict):

        def chmod(self, mode):
            chmod(str(self), mode)

        def decrypt(self, decrypt_args=''):
            proc = popen(str(self) + decrypt_args,
                         stdin=PIPE, shell=True)
            proc.communicate(password + '\n')
            # Warning, possible deadlock if more input is expected
            return proc.wait()

    return PathExt


def walk(path):
    if path.is_dir():
        root = path
        dirs, files = tee(root.iterdir())
        dirs = list(filter(lambda d: d.is_dir(), dirs))
        files = list(filter(lambda f: f.is_file(), files))
        yield root, dirs, files
        for subdir in dirs:
            for out in walk(subdir):
                yield out


class MockPopen(object):
    def __init__(self, path, stdin=None, stdout=None, stderr=None, shell=True):
        self.path = path
        self.stdout = StringIO('Enter Passphrase: ')
        self.stderr = StringIO()

    def communicate(self, s):
        log.info('MockPopen::communicate %s' % s)

    def wait(self):
        pass


class MockPath(object):
    fs = {
        '': {
            'over': {
                'there': {
                    'f1': 'stuff',
                    'res000050354req_f2': 'junk'}}}}

    def __init__(self, s):
        from posixpath import join as pathjoin
        self.pathjoin = lambda other: self.__class__(pathjoin(s, other))

        if not s.startswith('/'):
            raise NotImplementedError
        self.resolve = lambda: self

        self._path = s
        self.name = s.split('/')[-1]

    def __str__(self):
        return self._path

    def _lookup(self):
        parts = self._path.split('/')
        f = self.fs
        for part in parts:
            try:
                f = f[part]
            except KeyError:
                raise IOError(part)
        return f

    def is_dir(self):
        try:
            return isinstance(self._lookup(), dict)
        except IOError:
            return False

    def is_file(self):
        try:
            return isinstance(self._lookup(), str)
        except IOError:
            return False

    def iterdir(self):
        if self.is_dir():
            for k in sorted(self._lookup().keys()):
                yield self / k
        else:
            raise IOError(self)

    def __div__(self, other):
        return self.pathjoin(other)

    @classmethod
    def make_ext(cls, path,
                 password='sekret'):
        PathClass = mix_path_utils(MockPath, MockPopen, password, mock_chmod)
        return lambda: PathClass(path)


def mock_chmod(path, mode):
    log.info('mock_chmod: %s, %s' % (path, mode))


if __name__ == '__main__':
    def _tcb():
        from os import chmod, environ
        from subprocess import Popen
        from sys import argv

        from pathlib import Path

        (my_popen, my_chmod) = ((MockPopen, mock_chmod)
                                if '--dry-run' in argv
                                else (Popen, chmod))

        def arg_path():
            logging.basicConfig(
                format='%(asctime)s (%(levelname)s) %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
            [enc_path_name, pass_key] = argv[1:3]
            PathExt = mix_path_utils(Path, my_popen,
                                     environ[pass_key], my_chmod)
            return PathExt(enc_path_name)

        main(arg_path)
    _tcb()
