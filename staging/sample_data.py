''' Create sample data by taking the first n rows of input files.
'''
import logging  # Exception to OCAP
from contextlib import contextmanager
from functools import partial
from os.path import isfile, abspath, splitext, join as pjoin
from StringIO import StringIO

logging.basicConfig(format='%(asctime)s (%(levelname)s) %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
log = logging.getLogger(__name__)


def main(get_input_path, get_output_path, get_n_rows, list_files_argv,
         open_rd_argv, open_wrt_argv, pjoin, copy_file,
         data_file_ext=['.csv', '.dat']):

    for afile in list_files_argv(get_input_path()):
        src = pjoin(get_input_path(), afile)
        dest = pjoin(get_output_path(), afile)
        if splitext(afile)[1] in data_file_ext:
            copy_rows(open_rd_argv, src, open_wrt_argv, dest, get_n_rows())
        else:
            log.info('Copying %s to %s' % (src, dest))
            copy_file(src, dest)


def copy_rows(open_rd, src, open_wrt, dest, nrows):
    log.info('Copying %d rows from %s to %s' % (nrows, src, dest))
    with open_rd(src) as fin:
        with open_wrt(dest) as fout:
            for idx, row in enumerate(fin):
                if idx >= nrows:
                    break
                fout.write(row)


class mock_file(StringIO):
    def __init__(self, init=''):
        StringIO.__init__(self, init)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False


def mock_open(path, mode):
    log.info('mock_open %s (%s)' % (path, mode))
    return mock_file('mock file contents')


def mock_copyfile(src, dest):
    log.info('mock_copy_file %s %s)' % (src, dest))


def chk_path(valid_path, path):
    if valid_path not in path:
        raise RuntimeError('%s not in %s' % (valid_path, path))


if __name__ == '__main__':
    def _tcb():
        from os import listdir
        from shutil import copyfile
        from sys import argv

        def chk_exists(path):
            if isfile(path):
                raise RuntimeError('%s already exists!' % path)

        def get_input_path():
            return argv[1]

        def get_output_path():
            return argv[2]

        def get_n_rows():
            return int(argv[3])

        def list_files_argv(path):
            chk_path(get_input_path(), path)
            return [f for f in listdir(abspath(path))
                    if isfile(pjoin(path, f))]

        def copy_file(cf, src, dest):
            chk_path(get_input_path(), src)
            chk_path(get_output_path(), dest)
            chk_exists(dest)
            cf(src, dest)

        @contextmanager
        def open_argv(openf, mode, path):
            chk_path(get_output_path(), get_output_path())
            if 'w' in mode:
                chk_exists(path)
            with openf(path, mode) as f:
                yield f

        caps = dict(get_input_path=get_input_path,
                    get_output_path=get_output_path,
                    get_n_rows=get_n_rows,
                    list_files_argv=list_files_argv,
                    pjoin=pjoin)

        if '--dry-run' in argv:
            caps.update(dict(
                open_rd_argv=partial(open_argv, mock_open, 'rb'),
                open_wrt_argv=partial(open_argv, mock_open, 'wb'),
                copy_file=partial(copy_file, mock_copyfile)
                ))
        else:
            caps.update(dict(
                open_rd_argv=partial(open_argv, open, 'rb'),
                open_wrt_argv=partial(open_argv, open, 'wb'),
                copy_file=partial(copy_file, copyfile)
                ))

        main(**caps)

    _tcb()
