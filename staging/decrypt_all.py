''' Decrypt all CMS files - extracted files are placed in the working directory
'''
import stat
from functools import partial
from os.path import join as pjoin
from StringIO import StringIO
from subprocess import PIPE

import logging  # Exception to OCAP

logging.basicConfig(format='%(asctime)s (%(levelname)s) %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
log = logging.getLogger(__name__)


def main(mk_decrypt, mk_fops, get_enc_path, walk, getpass,
         isfile, abspath, filter_prefix,
         decrypt_args=' --overwrite --verbose'):
    fops = mk_fops()
    decrypt = mk_decrypt(password=getpass())

    decrypt_count = 0
    for root, dirs, files in walk(get_enc_path()):
        for name in files:
            full_path = abspath(pjoin(root, name))
            if isfile(full_path):
                if name.startswith(filter_prefix):
                    fops.chmod(full_path,
                               stat.S_IXUSR | stat.S_IRUSR | stat.S_IWUSR)
                    log.info('Decrypting %s' % full_path)
                    ret = decrypt.decrypt(full_path, decrypt_args)
                    if ret:
                        raise RuntimeError('Return %d from "%s"' %
                                           (ret, full_path))
                    decrypt_count += 1
                else:
                    log.info('Skipped due to filter: %s' % name)
    log.info('Decrypted %d files' % decrypt_count)


def mock_do_chmod(path, mode):
    log.info('chmod: %s, %s' % (path, mode))


class Decrypt(object):
    def __init__(self, popen, password, chk_path):
        self.password = password
        self.popen = popen
        self.chk_path = chk_path

    @classmethod
    def make(cls, popen, password, chk_path):
        return Decrypt(popen, password, chk_path)

    def decrypt(self, path, decrypt_args=''):
        self.chk_path(path)
        proc = self.popen(path + decrypt_args, stdin=PIPE, shell=True)
        proc.communicate(self.password + '\n')
        # Warning, possible deadlock if more input is expected
        return proc.wait()


class MockPopen(object):
    def __init__(self, path, stdin=None, stdout=None, stderr=None, shell=True):
        self.path = path
        self.stdout = StringIO('Enter Passphrase: ')
        self.stderr = StringIO()

    def communicate(self, s):
        log.info('MockPopen::communicate %s' % s)

    def wait(self):
        pass


class Fops(object):
    def __init__(self, chk_path, chmod):
        self.chk_path = chk_path
        self.chmod = chmod

    @classmethod
    def make(cls, chk_path, chmod):
        return Fops(chk_path, chmod)

    def chmod(self, path, mode):
        self.chk_path(path)
        self.chmod(path, mode)


def mock_chmod(path, mode):
    log.info('mock_chmod: %s, %s' % (path, mode))


if __name__ == '__main__':
    def _tcb(filter_prefix='res000050354req'):
        from os import walk, chmod, environ
        from os.path import isfile, abspath
        from subprocess import Popen
        from sys import argv

        # Path to where the delivered HD was copied.  Recursively search
        # for encrypted files matching the filter_prefix pattern.
        def get_enc_path():
            return abspath(argv[1])

        def getpass():
            return environ[argv[2]]

        def chk_path(path):
            if get_enc_path() not in path:
                raise RuntimeError('%s not in %s' % (
                    get_enc_path(), path))

        if '--dry-run' in argv:
            mk_fops = partial(Fops.make, chk_path=chk_path, chmod=mock_chmod)
            mk_decrypt = partial(Decrypt.make, popen=MockPopen,
                                 chk_path=chk_path)
        else:
            mk_fops = partial(Fops.make, chk_path=chk_path, chmod=chmod)
            mk_decrypt = partial(Decrypt.make, popen=Popen, chk_path=chk_path)

        main(mk_decrypt, mk_fops, get_enc_path, walk, getpass,
             isfile, abspath, filter_prefix)
    _tcb()
