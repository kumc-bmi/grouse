''' Decrypt all CMS files - extracted files are placed in the working directory
'''
import stat
from os.path import join as pjoin


def main(walk, getpass, isfile, abspath, chmod, path, filter_prefix, decrypt):
    password = getpass('Enter decryption key: ')
    decrypt_count = 0
    for root, dirs, files in walk(path):
        for name in files:
            full_path = abspath(pjoin(root, name))
            if isfile(full_path):
                if name.startswith(filter_prefix):
                    chmod(full_path, stat.S_IXUSR)
                    decrypt(full_path, password)
                    decrypt_count += 1
                else:
                    print 'Skipped due to filter: %s' % name
    print 'Decrypted %d files' % decrypt_count


def mock_do_chmod(path, mode):
    print 'chmod: %s, %s' % (path, mode)


def mock_do_decrypt(path, password):
    print 'decrypt %s' % path

if __name__ == '__main__':
    def _tcb(filter_prefix='res000050354req'):
        from os import walk, chmod
        from os.path import isfile, abspath
        from getpass import getpass
        from sys import argv

        # Path to where the delivered HD was copied.  Recursively search
        # for encrypted files matching the filter_prefix pattern.
        enc_path = argv[1]

        def do_decrypt(path, password):
            raise NotImplementedError()

        def do_chmod(path, mode):
            chmod(path, mode)

        if '--dry-run' in argv:
            my_do_chmod = mock_do_chmod
            my_do_decrypt = mock_do_decrypt
        else:
            my_do_chmod = do_chmod
            my_do_decrypt = do_decrypt

        main(walk, getpass, isfile, abspath, my_do_chmod,
             abspath(enc_path), filter_prefix, my_do_decrypt)
    _tcb()
