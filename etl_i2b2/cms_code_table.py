'''
>>> from urllib.request import build_opener
>>> web = build_opener()
>>> content = web.open(url1).read().decode('utf-8')
>>> print("'" + _claim_type(content).replace("'", "''") + "'")
... #doctest: +ELLIPSIS
'<table><item code="10" value="HHA claim" />
<item code="20" value="Non swing bed SNF claim" />
<item code="30" value="Swing bed SNF claim" />
...
<item code="82" value="RIC M DMERC DMEPOS claim" />
</table>'

'''

from hashlib import sha1
from pathlib import Path  # use the type only; the constructor is ambient authority
from sys import stderr
from typing import List, Tuple

from xml.etree import ElementTree as ET

url1 = 'https://www.resdac.org/cms-data/variables/medpar-nch-claim-type-code'
Item = Tuple[str, str]


def _claim_type(content: str) -> str:
    return _markup(_items(content))


def _items(content: str) -> List[Item]:
    _skip, content = content.split('<table ', 1)
    content, _skip = content.split('</table>', 1)
    content = '<table ' + content + '</table>'
    table = ET.fromstring(content)
    trs = table.findall('.//tr')
    rows = [[''.join(cell.itertext())
             for cell in tr.findall('.//td')]
            for tr in trs]
    items = [(code, value)
             for row in rows if row
             for (code, value) in [row]]
    return items


def _markup(items: List[Item]) -> str:
    table = ET.Element('table')
    for code, value in items:
        item = ET.SubElement(table, 'item',
                             code=code, value=value)
        item.tail = '\n'
    raw = ET.tostring(table)  # type: bytes
    return raw.decode('utf-8')


class Cache(object):
    def __init__(self, cache: Path, ua):

        def checksum(filename, expected):
            target = cache / filename
            if not (target).exists():
                raise IOError('no such file: %s' % target)
            actual = sha1(target.read_bytes()).hexdigest()
            if actual.strip() == expected.strip():
                return target
            else:
                raise IOError('bad checksum for %s:\n%s' % (target, actual))
        self.checksum = checksum

        def download(addr, sha1sum):
            filename = addr.rsplit('/', 1)[-1]
            target = cache / filename
            print('downloading:', addr, 'to', target, file=stderr)
            with ua.open(addr) as dl:
                target.write_bytes(dl.read())
            return checksum(filename, sha1sum)
        self.download = download

    def __getitem__(self, k):
        _label, addr, sha1sum = k
        try:
            filename = addr.rsplit('/', 1)[-1]
            return self.checksum(filename, sha1sum)
        except IOError:
            return self.download(addr, sha1sum)
