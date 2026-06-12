from collections.abc import AsyncIterable, AsyncIterator
from typing import Any, TypeVar, overload


# There are other asynchronous azip libraries out there
# but the maitnence of those can be questioned so here's
# our own version
T = TypeVar("T")
T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")

_T_co = TypeVar("_T_co", covariant=True)


# NOTE: AsyncIterator has an __anext__ function taken care of for us so it's not here...
class azip(AsyncIterator[_T_co]):
    __slots__ = ("_iters", "_strict", "_tuplesize")
    _ites: tuple[AsyncIterable[Any], ...]
    _strict: bool
    _tuplesize: int

    @overload
    def __init__(self: "azip[Any]") -> None: ...
    @overload
    def __init__(self: "azip[tuple[T1]]", iter1: AsyncIterable[T1], /): ...
    @overload
    def __init__(
        self: "azip[tuple[T1, T2]]",
        iter1: AsyncIterable[T1],
        iter2: AsyncIterable[T2],
        /,
    ) -> None: ...
    @overload
    def __init__(
        self: "azip[tuple[T1, T2, T3]]",
        iter1: AsyncIterable[T1],
        iter2: AsyncIterable[T2],
        iter3: AsyncIterable[T3],
        /,
    ) -> None: ...
    @overload
    def __init__(
        self: "azip[tuple[T1, T2, T3, T4]]",
        iter1: AsyncIterable[T1],
        iter2: AsyncIterable[T2],
        iter3: AsyncIterable[T3],
        iter4: AsyncIterable[T4],
        /,
        *,
        strict: bool = False,
    ): ...
    @overload
    def __init__(
        self: "azip[tuple[T1, T2, T3, T4, T5]]",
        iter1: AsyncIterable[T1],
        iter2: AsyncIterable[T2],
        iter3: AsyncIterable[T3],
        iter4: AsyncIterable[T4],
        iter5: AsyncIterable[T5],
        /,
        *,
        strict: bool = False,
    ): ...

    def __init__(
        self, *iters: AsyncIterable[Any], strict: bool = False
    ) -> "azip[tuple[Any, ...]]":
        self._iters = tuple(map(aiter, iters))
        self._strict = strict
        self._tuplesize = len(self._iters)

    async def __anext__(self) -> _T_co:
        # "i" needs to be kept in scope.
        i = 0
        try:
            results = []
            for i in range(self._tuplesize):
                results.append(await anext(self._iters[i]))
            return tuple(results)
        except StopAsyncIteration as err:
            if self._strict:
                # The diagnosis part can be the trickiest portion
                # Not even zip iteself. A Rough version of CPython's
                # C Version was enough to logically get the job
                # completed.

                if i:
                    plural = " " if i == 1 else "s 1-"
                    raise ValueError(
                        f"azip() argument {i + 1} is shorter than argument{plural}{i}"
                    )
                for i in range(1, self._tuplesize):
                    try:
                        await anext(self._iters[i])
                        plural = " " if i == 1 else "s 1-"
                        raise ValueError(
                            f"azip() argument {i + 1} is longer"
                            f" than argument{plural}{i}"
                        )
                    except StopAsyncIteration:
                        continue
            raise err
