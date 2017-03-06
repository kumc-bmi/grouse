from typing import Any, Callable, TypeVar, cast

import luigi


_T = TypeVar('_T')
_U = TypeVar('_U')


def _valueOf(example: _T, cls: Callable[..., _U]) -> Callable[..., _T]:

    def getValue(*args: Any, **kwargs: Any) -> _T:
        return cast(_T, cls(*args, **kwargs))
    return getValue


StrParam = _valueOf('s', luigi.Parameter)
IntParam = _valueOf(0, luigi.BoolParameter)
BoolParam = _valueOf(True, luigi.BoolParameter)
