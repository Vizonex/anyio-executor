import sys
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Generator,
    Iterable,
    Iterator,
    Sequence,
)
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from types import TracebackType
from typing import Any, Generic, TypeVar, final, overload

import anyio
from aiologic import CountdownEvent, SimpleQueue

from .azip import azip


if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self


R = TypeVar("R")
T = TypeVar("T")
T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")


@dataclass(slots=True)
class _WorkItem(Generic[T]):
    func: Callable[[], Awaitable[T]]
    exc_handler: Callable[[BaseException], None] | None
    queue: SimpleQueue[T]

    async def execute(self) -> None:
        try:
            await self.queue.async_put(await self.func())
        except Exception as e:
            if self.exc_handler:
                self.exc_handler(e)
            else:
                raise e


@dataclass(slots=True)
class ExecutorResult(Awaitable[list[T]], AsyncIterable[T]):
    """used for waiting upon results to return in different asynchronous
    fashions."""

    queue: SimpleQueue[T]
    count: CountdownEvent
    executor: "Executor"

    async def __aiter__(self) -> AsyncIterator[T]:
        while not self.executor.is_closed:
            item = await self.queue.async_get()
            yield item

            self.count.down()
            if bool(self.count):
                break

    async def __wait__(self) -> list[T]:
        return [i async for i in self]

    def __await__(self) -> Generator[Any, None, list[T]]:
        return self.__wait__().__await__()


# inspired by aiolibs_executor.


@final
class Executor:
    """A small, yet effective and efficient asynchronous executor object."""

    __slots__ = (
        "__weakref__",
        "_num_workers",
        "_work_queue",
        "_exc_handler",
        "_tg",
        "_workers_done",
    )

    def __init__(
        self,
        num_workers: int = 0,
        exc_handler: Callable[[BaseException], None] | None = None,
    ) -> None:
        """
        :param num_workers:
            A number of worker tasks to provide defaults to 16.
        :param exc_handler:
            An optional exception handler to call if a function
            executed or submitted fails.
        """

        if num_workers < 0:
            raise ValueError("num_wokers must be a positive integer")
        if num_workers == 0:
            num_workers = 16

        self._num_workers = num_workers
        self._work_queue: SimpleQueue[_WorkItem[T] | None] = SimpleQueue()
        self._exc_handler = exc_handler
        self._tg = anyio.create_task_group()
        self._workers_done = CountdownEvent()

    @property
    def is_closed(self) -> bool:
        """checks if executor is not currently running"""
        return bool(self._workers_done)

    async def _start(self) -> None:
        for _ in range(self._num_workers):
            await self._tg.start(self._work)

    async def __aenter__(self) -> Self:
        await self._tg.__aenter__()
        await self._start()
        return self

    @contextmanager
    def _worker_scope(self) -> Iterator[None]:
        """Assistance with Lazy event up/down event functionality."""
        self._workers_done.up()
        try:
            yield
        finally:
            self._workers_done.down()

    async def _work(self, task_status=anyio.TASK_STATUS_IGNORED) -> None:
        task_status.started()
        with self._worker_scope():
            while item := await self._work_queue.async_get():
                await item.execute()

    # seperate tasks used for task queing to
    # allow for simultaneious memory loading
    # without eating up too much iterators
    # will immediately run alongside which
    # is what we want.
    async def _load_tasks_sync_iter(
        self,
        queue: SimpleQueue[T],
        counter: CountdownEvent,
        func: Callable[..., Awaitable[R] | Coroutine[Any, Any, R]],
        exc_handler: Callable[[BaseException], None] | None,
        it: Iterable[Any],
        /,
        *its: Iterable[Any],
    ) -> None:
        for args in zip(it, *its, strict=False):
            counter.up()
            await self._work_queue.async_put(
                _WorkItem(partial(func, *args), exc_handler, queue)
            )
        # we used a single up to ensure ExecutorResult
        # can't be shut down before results come in.
        counter.down()

    async def _load_tasks_async_iter(
        self,
        queue: SimpleQueue[T],
        counter: CountdownEvent,
        func: Callable[..., Awaitable[R] | Coroutine[Any, Any, R]],
        exc_handler: Callable[[BaseException], None] | None,
        it: AsyncIterable[Any],
        /,
        *its: AsyncIterable[Any],
    ) -> None:
        # A Custom azip object was made just for asynchronous packing.
        # It even has an unused feature for diagnosing using strict
        # (remains unused).

        async for args in azip(it, *its, strict=False):
            counter.up()
            await self._work_queue.async_put(
                _WorkItem(partial(func, *args), exc_handler, queue)
            )
        counter.down()

    @overload
    def map(
        self,
        func: Callable[[T1], Awaitable[R] | Coroutine[Any, Any, R]],
        it1: Iterable[T1] | Sequence[T1],
        /,
        *,
        exc_handler: Callable[[BaseException], None] | None = None,
    ) -> ExecutorResult[R]: ...

    @overload
    def map(
        self,
        func: Callable[[T1, T2], Awaitable[R] | Coroutine[Any, Any, R]],
        it1: Iterable[T1] | Sequence[T1],
        it2: Iterable[T2] | Sequence[T2],
        /,
        *,
        exc_handler: Callable[[BaseException], None] | None = None,
    ) -> ExecutorResult[R]: ...

    @overload
    def map(
        self,
        func: Callable[[T1, T2, T3], Awaitable[R] | Coroutine[Any, Any, R]],
        it1: Iterable[T1] | Sequence[T1],
        it2: Iterable[T2] | Sequence[T2],
        it3: Iterable[T3] | Sequence[T3],
        /,
        *,
        exc_handler: Callable[[BaseException], None] | None = None,
    ) -> ExecutorResult[R]: ...

    @overload
    def map(
        self,
        func: Callable[[T1, T2, T3, T4], Awaitable[R] | Coroutine[Any, Any, R]],
        it1: Iterable[T1] | Sequence[T1],
        it2: Iterable[T2] | Sequence[T2],
        it3: Iterable[T3] | Sequence[T3],
        it4: Iterable[T4] | Sequence[T4],
        /,
        *,
        exc_handler: Callable[[BaseException], None] | None = None,
    ) -> ExecutorResult[R]: ...

    @overload
    def map(
        self,
        func: Callable[[T1, T2, T3, T4, T5], Awaitable[R] | Coroutine[Any, Any, R]],
        it1: Iterable[T1] | Sequence[T1],
        it2: Iterable[T2] | Sequence[T2],
        it3: Iterable[T3] | Sequence[T3],
        it4: Iterable[T4] | Sequence[T4],
        it5: Iterable[T5] | Sequence[T5],
        /,
        *,
        exc_handler: Callable[[BaseException], None] | None = None,
    ) -> ExecutorResult[R]: ...

    def map(
        self,
        func: Callable[..., Awaitable[R] | Coroutine[Any, Any, R]],
        it: Iterable[Any] | Sequence[Any],
        /,
        *its: Iterable[Any] | Sequence[Any],
        exc_handler: Callable[[BaseException], None] | None = None,
    ) -> ExecutorResult[R]:
        """Executes an asynchronous function concurrently with
        synchronous iterables to use.

        :param func: The function to run.
        :param exc_handler: Provides an alternative solution
            for handling given exceptions, if not selected
            for this function will back off to whatever
            the executor class has provided and if that
            is not the case then the exception will
            be raised.

        :raise RuntimeError:
            If executor has been previously shut down.
        """
        if self.is_closed:
            raise RuntimeError("Executor already closed.")
        cd = CountdownEvent()
        cd.up()
        queue: SimpleQueue[T] = SimpleQueue()
        self._tg.start_soon(
            self._load_tasks_sync_iter,
            queue,
            cd,
            func,
            exc_handler or self._exc_handler,
            it,
            *its,
        )
        return ExecutorResult(queue, cd, self)

    @overload
    def amap(
        self,
        func: Callable[[T1], Awaitable[R] | Coroutine[Any, Any, R]],
        it1: AsyncIterable[T1],
        /,
        *,
        exc_handler: Callable[[BaseException], None] | None = None,
    ) -> ExecutorResult[R]: ...

    @overload
    def amap(
        self,
        func: Callable[[T1, T2], Awaitable[R] | Coroutine[Any, Any, R]],
        it1: AsyncIterable[T1],
        it2: AsyncIterable[T2],
        /,
        *,
        exc_handler: Callable[[BaseException], None] | None = None,
    ) -> ExecutorResult[R]: ...

    @overload
    def amap(
        self,
        func: Callable[[T1, T2, T3], Awaitable[R] | Coroutine[Any, Any, R]],
        it1: AsyncIterable[T1],
        it2: AsyncIterable[T2],
        it3: AsyncIterable[T3],
        /,
        *,
        exc_handler: Callable[[BaseException], None] | None = None,
    ) -> ExecutorResult[R]: ...

    @overload
    def amap(
        self,
        func: Callable[[T1, T2, T3, T4], Awaitable[R] | Coroutine[Any, Any, R]],
        it1: AsyncIterable[T1],
        it2: AsyncIterable[T2],
        it3: AsyncIterable[T3],
        it4: AsyncIterable[T4],
        /,
        *,
        exc_handler: Callable[[BaseException], None] | None = None,
    ) -> ExecutorResult[R]: ...

    @overload
    def amap(
        self,
        func: Callable[[T1, T2, T3, T4, T5], Awaitable[R] | Coroutine[Any, Any, R]],
        it1: AsyncIterable[T1],
        it2: AsyncIterable[T2],
        it3: AsyncIterable[T3],
        it4: AsyncIterable[T4],
        it5: AsyncIterable[T5],
        /,
        *,
        exc_handler: Callable[[BaseException], None] | None = None,
    ) -> ExecutorResult[R]: ...

    def amap(
        self,
        func: Callable[..., Awaitable[R] | Coroutine[Any, Any, R]],
        it: AsyncIterable[Any],
        /,
        *its: AsyncIterable[Any],
        exc_handler: Callable[[BaseException], None] | None = None,
    ) -> ExecutorResult[R]:
        """
        Executes an asynchronous function concurrently with
        asynchronous iterables to use.

        :param func: The function to run.
        :param exc_handler: Provides an alternative solution
            for handling given exceptions, if not selected
            for this function will back off to whatever
            the executor class has provided and if that
            is not the case then the exception will
            be raised.

        :raise RuntimeError:
            If executor has been previously shut down.
        """
        if self.is_closed:
            raise RuntimeError("Executor already closed.")

        queue: SimpleQueue[T] = SimpleQueue()
        cd = CountdownEvent()
        cd.up()
        queue: SimpleQueue[T] = SimpleQueue()
        self._tg.start_soon(
            self._load_tasks_async_iter,
            queue,
            cd,
            func,
            exc_handler or self._exc_handler,
            it,
            *its,
        )
        return ExecutorResult(queue, cd, self)

    async def shutdown(self) -> None:
        """Shuts down all running executor tasks"""
        if not self.is_closed:
            for _ in range(self._num_workers):
                # Set all workers to completed...
                await self._work_queue.async_put(None)
            await self._workers_done

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ):
        await self.shutdown()
        return await self._tg.__aexit__(exc_type, exc_val, exc_tb)


__all__ = ("Executor", "ExecutorResult")
