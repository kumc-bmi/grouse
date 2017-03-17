'''eventlog -- structured logging support

EventLogger for nested events
=============================

Suppose we want to log some events which naturally nest.

First, add a handler to a logger and set its level to DEBUG to see
event starts as well as their outcomes:

>>> import logging, sys
>>> log1 = logging.getLogger('log1')
>>> detail = logging.StreamHandler(sys.stdout)
>>> log1.addHandler(detail)
>>> log1.setLevel(logging.INFO)
>>> detail.setFormatter(logging.Formatter(
...     fmt='%(levelname)s%(elapsed)s%(step)s %(message)s'))

>>> io = MockIO()
>>> event0 = EventLogger(log1, dict(customer='Jones', invoice=123), io.clock)

>>> event0.info('Build %(product)s...', dict(product='house'))
INFO('2000-01-01 12:30:01', '0:00:02', 2000000)[1] Build house...

>>> with event0.step('lay foundation %(depth)d ft deep', dict(depth=20)) as t:
...     event0.info('foundation complete')
INFO('2000-01-01 12:30:06', '0:00:04', 4000000)[2, 1] lay foundation 20 ft deep
INFO('2000-01-01 12:30:06', '0:00:09', 9000000)[2, 2] foundation complete

>>> with event0.step('frame %(stories)d story house', dict(stories=2)) as t:
...     event0.info('framed %(stories)d story house', dict(stories=2))
INFO('2000-01-01 12:30:21', '0:00:07', 7000000)[3, 1] frame 2 story house
INFO('2000-01-01 12:30:21', '0:00:15', 15000000)[3, 2] framed 2 story house

'''

from contextlib import contextmanager
from datetime import datetime
from typing import (
    Any, Callable, Dict, Iterator, MutableMapping, Optional as Opt, Tuple
)
import logging

KWArgs = MutableMapping[str, Any]
JSONObject = Dict[str, Any]


class EventLogger(logging.LoggerAdapter):
    def __init__(self, logger: logging.Logger, event: JSONObject,
                 clock: Opt[Callable[[], datetime]]=None) -> None:
        logging.LoggerAdapter.__init__(self, logger, extra={})
        if clock is None:
            clock = datetime.now  # ISSUE: ambient
        self.event = event
        self._clock = clock
        self._step = [(0, clock())]

    def process(self, msg: str, kwargs: KWArgs) -> Tuple[str, KWArgs]:
        step_ix, then = self._step[-1]
        self._step[-1] = (step_ix + 1, then)
        extra = dict(kwargs.get('extra', {}),
                     elapsed=self.elapsed(then=then),
                     event=self.event,
                     step=[ix for [ix, _t] in self._step])
        return msg, dict(kwargs, extra=extra)

    def elapsed(self, then: Opt[datetime]=None) -> Tuple[str, str, int]:
        start = then or self._step[-1][1]
        elapsed = self._clock() - start
        ms = int(elapsed.total_seconds() * 1000000)
        return (str(start), str(elapsed), ms)

    @contextmanager
    def step(self, msg: str, argobj: Dict[str, object]) -> Iterator[datetime]:
        checkpoint = self._clock()
        step_ix, then = self._step[-1]
        self._step[-1] = (step_ix + 1, then)
        self._step.append((0, checkpoint))
        self.info(msg, argobj)
        try:
            yield checkpoint
        finally:
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
