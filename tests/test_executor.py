from collections.abc import Callable
from unittest import mock

import anyio
import pytest

from anyio_executor.core import Executor


async def noop(*args):
    return


async def fast_ret(i: int) -> int:
    return i


class BaseExecutorTest:
    def executor(self) -> Executor:
        return Executor()

    def executor_with_handler(
        self, handler:Callable[[BaseException], None]
    ) -> Executor:
        return Executor(exc_handler=handler)


class TestInvalidStates(BaseExecutorTest):
    @pytest.mark.anyio
    async def test_invalid_worker_amount(self):
        with pytest.raises(ValueError, match=r"num_wokers must be a positive integer"):
            _ = Executor(-69)

    @pytest.mark.anyio
    async def test_bad_map_before_entrance(self):
        e = self.executor()
        with pytest.raises(RuntimeError, match=r"Executor already closed."):
            await e.map(noop, [1, 2, 3])

    @pytest.mark.anyio
    async def test_bad_amap_before_entrance(self):
        e = self.executor()

        async def fake_aiter():
            for i in range(3):
                yield i

        with pytest.raises(RuntimeError, match=r"Executor already closed."):
            await e.amap(noop, fake_aiter())

    @pytest.mark.anyio
    async def test_bad_map_after_shutdown(self):
        async with self.executor() as e:
            await e.shutdown()
            with pytest.raises(RuntimeError, match=r"Executor already closed."):
                await e.map(noop, [1, 2, 3])

    @pytest.mark.anyio
    async def test_bad_amap_after_shutdown(self):
        async with self.executor() as e:
            await e.shutdown()

            async def fake_aiter():
                for i in range(3):
                    yield i

            with pytest.raises(RuntimeError, match=r"Executor already closed."):
                await e.amap(noop, fake_aiter())

@pytest.mark.anyio

class TestExecutor(BaseExecutorTest):
    async def wait(self, item: int) -> int:
        await anyio.sleep(item / 1000)
        return item

    @pytest.mark.anyio
    async def test_map_await(self):
        async with self.executor() as e:
            items = [i for i in range(1, 5)]
            i = await e.map(self.wait, items)
        # it doesn't matter what order they come back
        # in (as long as they all come back of course)
        assert set(i) == {1, 2, 3, 4}

    @pytest.mark.anyio
    async def test_map_aiter(self):
        async with self.executor() as e:
            items = [i for i in range(1, 5)]
            async for i in e.map(fast_ret, items):
                assert i in [1, 2, 3, 4]

    # NOTE: Sometimes trio will attempt to execute things
    # differently  example: [4, 1, 2, 3]
    # as long as they all come return back to us then it did
    # it's job.
    @pytest.mark.anyio
    async def test_amap_await(self):
        async def ait():
            for i in range(1, 5):
                yield i

        async with self.executor() as e:
            i = await e.amap(self.wait, ait())
        assert set(i) == {1, 2, 3, 4}

    @pytest.mark.anyio
    async def test_amap_aiter(self):
        async def ait():
            for i in range(1, 5):
                yield i

        async with self.executor() as e:
            async for i in e.amap(fast_ret, ait()):
                assert i in [1, 2, 3, 4]

    @pytest.mark.anyio
    async def test_exception_handler(self):
        callback = mock.Mock()

        class TornadoError(Exception):
            pass

        def on_exception(exc: Exception):
            assert isinstance(exc, TornadoError)
            callback(exc)

        async def func(i: int):
            raise TornadoError("Tornado approching...") 

        async with self.executor_with_handler(on_exception) as e:
            ret = await e.map(func, (1,))
        assert not ret
        assert callback.called
        assert callback.call_args.args
        assert isinstance(callback.call_args.args[0], TornadoError)

 