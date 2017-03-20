'''eventlog -- structured logging support

EventLogger for nested events
=============================

Suppose we want to log some events which naturally nest.

First, add a handler to a logger:

>>> import logging, sys
>>> log1 = logging.getLogger('log1')
>>> detail = logging.StreamHandler(sys.stdout)
>>> log1.addHandler(detail)
>>> log1.setLevel(logging.INFO)
>>> detail.setFormatter(logging.Formatter(
...     fmt='%(levelname)s %(elapsed)s %(do)s %(step)s %(message)s'))

>>> io = MockIO()
>>> event0 = EventLogger(log1, dict(customer='Jones', invoice=123), io.clock)


>>> with event0.step('Build %(product)s...', dict(product='house')):
...     with event0.step('lay foundation %(depth)d ft deep',
...                      dict(depth=20)) as info:
...         info.msg_parts.append(' at %(temp)d degrees')
...         info.argobj['temp'] = 65
...     with event0.step('frame %(stories)d story house',
...                      dict(stories=2)) as info:
...         pass
... # doctest: +ELLIPSIS
INFO ('... 12:30:01', None, None) begin [1] Build house...
INFO ('... 12:30:03', None, None) begin [1, 2] lay foundation 20 ft deep
INFO ('... 12:30:03', '0:00:03', 3000000) end [1, 2] lay ... deep at 65 degrees
INFO ('... 12:30:10', None, None) begin [1, 3] frame 2 story house
INFO ('... 12:30:10', '0:00:05', 5000000) end [1, 3] frame 2 story house
INFO ('... 12:30:01', '0:00:20', 20000000) end [1] Build house...

'''

from contextlib import contextmanager
from datetime import datetime
from typing import (
    Any, Callable, Dict, Iterator, List, MutableMapping,
    NamedTuple, Optional as Opt, Tuple
)
import logging

KWArgs = MutableMapping[str, Any]
JSONObject = Dict[str, Any]

LogState = NamedTuple('LogState', [
    ('msg_parts', List[str]),
    ('argobj', JSONObject),
    ('extra', JSONObject)])


class EventLogger(logging.LoggerAdapter):
    def __init__(self, logger: logging.Logger, event: JSONObject,
                 clock: Opt[Callable[[], datetime]]=None) -> None:
        logging.LoggerAdapter.__init__(self, logger, extra={})
        if clock is None:
            clock = datetime.now  # ISSUE: ambient
        self.event = event
        self._clock = clock
        self._seq = 0
        self._step = []  # type: List[Tuple[int, datetime]]
        List  # let flake8 know we're using it

    def process(self, msg: str, kwargs: KWArgs) -> Tuple[str, KWArgs]:
        extra = dict(kwargs.get('extra', {}),
                     event=self.event,
                     step=[ix for [ix, _t] in self._step])
        return msg, dict(kwargs, extra=extra)

    def elapsed(self, then: Opt[datetime]=None) -> Tuple[str, str, int]:
        start = then or self._step[-1][1]
        elapsed = self._clock() - start
        ms = int(elapsed.total_seconds() * 1000000)
        return (str(start), str(elapsed), ms)

    @contextmanager
    def step(self, msg: str, argobj: Dict[str, object],
             extra: Opt[Dict[str, object]]=None) -> Iterator[LogState]:
        checkpoint = self._clock()
        self._seq += 1
        self._step.append((self._seq, checkpoint))
        extra = extra or {}
        self.info(msg, argobj,
                  extra=dict(extra, do='begin',
                             elapsed=(str(checkpoint), None, None)))
        msgparts = [msg]
        try:
            yield LogState(msgparts, argobj, extra)
        finally:
            self.info(''.join(msgparts), argobj,
                      extra=dict(extra, do='end',
                                 elapsed=self.elapsed(then=checkpoint)))
            self._step.pop()


class MockIO(object):
    def __init__(self,
                 now: datetime=datetime(2000, 1, 1, 12, 30, 0)) -> None:
        self._now = now
        self._delta = 1

    def clock(self) -> datetime:
        from datetime import timedelta
        self._now += timedelta(seconds=self._delta)
        self._delta += 1
        return self._now
