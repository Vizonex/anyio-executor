# Anyio-Executor
Inspired by the newer [aiolibs-executor](https://github.com/aio-libs/aiolibs-executor) library
this library provides an asynchronous executor support for anyio and has compatability
with 3.10 and newer version of python.


```python
import anyio

from anyio_executor import Executor


async def fn(i: int):
    await anyio.sleep(i)
    print(f"{i} done!")
    return i

async def main():
    async with Executor(10) as e:
        items = await e.map(fn, [1, 2, 1, 2, 3])
    print(items)

    async with Executor(10) as e:
        # it will also take async for iterations.
        async for i in e.map(fn, [1, 2, 1, 2, 3]):
            print(i)

if __name__ == "__main__":
    anyio.run(main)
```
